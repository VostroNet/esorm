
import waterfall from "./waterfall";
import moment from "moment";

// import * as Types from "./types";

import {processElasticResponse} from "./utils";

export default class Model {
  constructor(values) {
    Object.assign(this, values);
  }
  static init(schema, options = {}) {
    if (!options.esorm) {
      throw new Error("No EsORM instance passed");
    }
    this.esorm = options.esorm;
    if (!options.modelName) {
      options.modelName = this.name;
    }
    this.modelName = options.modelName;
    let indexPrefix = this.esorm.config.indexPrefix || "";
    this.indexName = `${indexPrefix}${(options.indexName || this.modelName || this.name)}`.toLowerCase();
    this.typeName = options.typeName;
    this.schema = schema;
    this.hooks = schema.hooks;
    this.options = options;
  }
  static getQueryIndex() {
    return `${this.indexName}*`;
  }
  static async sync(options) {
    const client = await this.esorm.getClient();
    const indexesFound = await this.esorm.findIndex(`${this.indexName}*`);
    if (indexesFound.length > 0 && options.force) {
      await waterfall(indexesFound, (index) => {
        return client.indices.delete({
          index,
        }).then(processElasticResponse);
      });
    }

    const writeIndexName = await this.getWriteIndexName();
    await this.testWriteIndex(writeIndexName);

    if (this.schema.rollups) {
      this.rollups = await waterfall(Object.keys(this.schema.rollups), async(rollupName, r) => {
        const rollupIndexName = `rollup_${this.indexName}_${rollupName}`.toLowerCase();
        if (options.force) {
          let indexExists = await client.indices.exists({
            index: rollupIndexName,
          }).then(processElasticResponse);
          if (indexExists && options.force) {
            await client.indices.delete({
              index: rollupIndexName,
            }).then(processElasticResponse);
          }
        }
        await waterfall(Object.keys(this.schema.rollups[rollupName]), async(jobName) => {
          const jobId = `${rollupName}_${jobName}`.toLowerCase();
          const jobConfig = this.schema.rollups[rollupName][jobName];
          const {jobs} = await client.rollup.getJobs({
            id: jobId,
          }).then(processElasticResponse);
          let resetJob = false;
          if (jobs.length > 0 && options.force) {
            await client.rollup.stopJob({
              id: jobId,
            }).then(processElasticResponse);
            await client.rollup.deleteJob({
              id: jobId,
            }).then(processElasticResponse);
            resetJob = true;
          }
          if (resetJob || jobs.length === 0) {
            await client.rollup.putJob({
              id: jobId,
              body: Object.assign({
                index_pattern: `${this.indexName}*`, //eslint-disable-line
                rollup_index: rollupIndexName, //eslint-disable-line
              }, jobConfig),
            }).then(processElasticResponse);
            await client.rollup.startJob({
              id: jobId,
            }).then(processElasticResponse);
          }
        });

        r[rollupName] = rollupIndexName;
        return r;

      }, {});
    }
  }

  static async internalSearch(options) {
    const client = await this.esorm.getClient();
    if (this.rollups && !options.queryMainIndex) {
      return client.rollup.rollup_search(options)
        .then(processElasticResponse);
    } else {
      return client.search(options)
        .then(processElasticResponse);
    }
  }
  static async findAll(options) {
    let opts = await runHook(this, "beforeFind", options, undefined, options);
    const response = await this.internalSearch({
      index: this.getQueryIndex(),
      body: {
        query: opts.query,
        from: opts.from,
        size: opts.size,
        sort: opts.sort,
        aggs: opts.aggs || opts.aggregations,
      },
    });
    let models;
    if (!opts.raw) {
      models = response.hits.hits.map((hit) =>{
        return new this(Object.assign({
          id: hit._id, //eslint-disable-line
          _meta: {
            score: hit._score,//eslint-disable-line
            type: hit._type,//eslint-disable-line
          },
        }, hit._source));//eslint-disable-line
      });
    } else {
      models = response.hits.hits;
    }
    return runHook(this, "afterFind", options, undefined, {
      _meta: {
        shards: response._shards,//eslint-disable-line
        maxScore: response.hits.maxScore,
        total: response.hits.total,
        timedOut: response.hits.timed_out,
        took: response.hits.took,
      },
      models,
    }, opts);
  }
  static async query(options) {
    let opts = await runHook(this, "beforeQuery", options, undefined, options);
    const response = await this.internalSearch({
      index: this.getQueryIndex(),
      body: opts,
    });
    return runHook(this, "afterQuery", opts, undefined, response, opts);
  }
  static async count(options = {}) {
    let opts = await runHook(this, "beforeCount", options, undefined, options);
    const client = await this.esorm.getClient();
    const response = await client.count({
      index: this.getQueryIndex(),
      body: {
        query: opts.query,
        from: opts.from,
        sort: opts.sort,
        aggs: opts.aggs,
      },
    }).then(processElasticResponse);
    return runHook(this, "afterCount", opts, undefined, response.count, opts);
  }
  static getWriteIndexName() {
    if (this.schema.rotateIndex) {
      return `${this.indexName}-${moment().format(this.schema.indexFormat || "YYYYMMDD")}`;
    } else {
      return `${this.indexName}`;
    }
  }
  static async testWriteIndex(writeIndexName) {
    if (this.currentWriteIndex !== writeIndexName) {
      const client = await this.esorm.getClient();
      const indexes = await this.esorm.findIndex(writeIndexName);
      if (indexes.length === 0) {
        await client.indices.create({
          index: writeIndexName,
          include_type_name: true, //eslint-disable-line
          body: {
            mappings: {
              "_doc": {
                "properties": this.schema.mappings,
              },
            },
            settings: this.schema.settings,
          },
        }).then(processElasticResponse);
      }
      this.currentWriteIndex = writeIndexName;
    }
  }
  static async createBulk(records = [], options) {
    let r = await runHook(this, "beforeCreateBulk", options, undefined, records, options = {});

    const writeIndexName = this.getWriteIndexName();
    await this.testWriteIndex(writeIndexName);
    //TODO: move this to a running stream/queue?
    const newData = r.reduce((arr, val) => {
      arr.push({
        "index": {
          "_index": writeIndexName,
          "_type": "_doc",
        },
      });
      arr.push(val);
      return arr;
    }, []);
    let result;
    const client = await this.esorm.getClient();
    result = await client.bulk({
      refresh: true,
      body: newData,
    }).then(processElasticResponse);
    return runHook(this, "afterCreateBulk", options, undefined, result.items.map((i) => i.index), options);
  }
}

async function runHook(model, hookName, options = {}, context, initialArg, ...extra) {
  let hooks = [].concat();
  if (model.hooks) {
    if (model.hooks[hookName]) {
      hooks = hooks.concat(model.hooks[hookName]);
    }
  }
  if (options.hooks) {
    if (options.hooks[hookName]) {
      hooks = hooks.concat(options.hooks[hookName]);
    }
  }
  if (hooks.length > 0) {
    return waterfall(hooks, (hook, prevVal) => {
      return hook.apply(context, [prevVal].concat(extra || []));
    }, initialArg);
  }
  return initialArg;
}
