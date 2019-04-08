
import waterfall from "./waterfall";

import * as Types from "./types";

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
    this.indexName = options.indexName;
    this.typeName = options.typeName;
    this.schema = schema;
    this.hooks = schema.hooks;
    this.options = options;
  }
  static async findAll(options) {
    let opts = await runHook(this, "beforeFindInitial", options, undefined, options);
    const ql = this.createQueryString(opts);
    await runHook(this, "beforeFind", options, undefined, opts, ql);
    // ql.addField.apply(ql, Object.keys(this.schema.fields));
    // ql.addField.apply(ql, this.schema.tags);
    const response = await this.esorm.search({
      index: this.indexName,
      type: this.typeName,
      body: options.where
    });
    let models;
    if (!options.raw) {
      models = response.hit.hits.map((r) => new this(r));
    } else {
      models = response;
    }
    return runHook(this, "afterFind", options, undefined, models, opts);
  }

  static async count(options) {
    let opts = await runHook(this, "beforeCountInitial", options, undefined, options);
    const ql = this.createQueryString(opts);
    await runHook(this, "beforeCount", options, undefined, options, ql);
    ql.addFunction("count", Object.keys(this.schema.fields)[0]);
    ql.subQuery();
    ql.addFunction("sum", "count");
    await runHook(this, "beforeCountArgsSet", options, undefined, options, ql);
    const results = await this.inflx.query(ql.toSelect(), {
      precision: options.precision,
      retentionPolicy: options.retentionPolicy,
      database: options.database,
    });
    let fullCount = 0;
    if (results.length > 0) {
      fullCount = results[0].sum;
    }
    return runHook(this, "afterCount", options, undefined, fullCount, opts);
  }
  static async createBulk(records = [], options) {
    let r = await runHook(this, "beforeCreateBulk", options, undefined, records, options = {});
    const conn = this.inflx.getConnection();
    const newData = r.map((record) => {
      return {
        measurement: this.schema.measurement,
        tags: this.schema.tags.reduce((o, tag) => {
          if (record[tag] !== undefined && record[tag] !== "") {
            o[tag] = record[tag];
          }
          return o;
        }, {}),
        fields: Object.keys(this.schema.fields).reduce((o, f) => {
          if (record[f] !== undefined && record[f] !== "") {
            o[f] = record[f];
          }
          return o;
        }, {}),
        timestamp: record.time,
      };
    });
    try {
      await conn.writePoints(newData, {
        database: options.database,
        precision: options.precision,
        retentionPolicy: options.retentionPolicy,
      });
    } catch(err) {
      console.log("influx error", err);
    }
    return runHook(this, "afterCreateBulk", options, undefined, records, options);
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
