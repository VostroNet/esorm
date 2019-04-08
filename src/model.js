
import waterfall from "./waterfall";

import * as Types from "./types";

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
    this.indexName = (options.indexName || this.modelName || this.name).toLowerCase();
    this.typeName = options.typeName;
    this.schema = schema;
    this.hooks = schema.hooks;
    this.options = options;
  }
  static async findAll(options) {
    let opts = await runHook(this, "beforeFind", options, undefined, options);
    const client = await this.esorm.getClient();
    const response = await client.search({
      index: this.indexName,
      body: {
        query: options.query,
        from: options.from,
        size: options.size,
        sort: options.sort,
        aggs: options.aggs,
      },
    }).then(processElasticResponse);
    let models;
    if (!options.raw) {
      models = response.hits.hits.map((hit) =>{
        return new this(Object.assign({
          id: hit._id, //eslint-disable-line
          _meta: {
            score: hit._score,//eslint-disable-line
            type: hit._type,//eslint-disable-line
          }
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

  static async count(options) {
    let opts = await runHook(this, "beforeCount", options, undefined, options);
    const client = await this.esorm.getClient();
    const response = await client.count({
      index: this.indexName,
      body: opts,
    }).then(processElasticResponse);
    return runHook(this, "afterCount", options, undefined, response.count, opts);
  }
  static async createBulk(records = [], options) {
    let r = await runHook(this, "beforeCreateBulk", options, undefined, records, options = {});
    const client = await this.esorm.getClient();
    //TODO: move this to a running stream/queue?
    const newData = r.reduce((arr, val) => {
      arr.push({
        "index": {
          "_index": this.indexName,
          "_type": "_doc",
        },
      });
      arr.push(val);
      return arr;
    }, []);
    let result;
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
