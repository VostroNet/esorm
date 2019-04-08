import {Client} from "@elastic/elasticsearch";
import logger from "./logger";
import Model from "./model";
import waterfall from "./waterfall";

import * as types from "./types";

const log = logger("index");

export const Types = types;

export default class EsORM {
  constructor(config) {
    this.models = {};
    this.config = config;
  }
  async createClient() {
    log.debug(`creating client ${this.config}`);
    this.client = new Client(this.config);
  } 
  async getClient() {
    if (!this.client) {
      await this.createClient();
    }
    return this.client;
  }
  async sync() {

    const client = await this.getClient();
    return waterfall(Object.keys(this.models), async(modelName) => {
      const model = this.models[modelName];
      const indexExists = await client.indices.exists({
        index: model.indexName,
      });
      if (!indexExists) {
        await client.indices.create({
          index: model.indexName,
          mappings: model.options.mappings || {
            "_doc": {
              properties: model.fields,
            }
          }
        });
      }
    });
  }
  define(modelName, schema, options = {}) {
    options.modelName = modelName;
    options.esorm = this;
    const model = class extends Model {};
    model.init(schema, options);
    this.models[modelName] = model;
    return model;
  }
  search(query) {
    const client = this.getClient();
    log.info("query", query);
    return client.search(query);
  }
}