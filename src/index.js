import {Client} from "@elastic/elasticsearch";
import logger from "./logger";
import Model from "./model";
import waterfall from "./waterfall";
import {processElasticResponse} from "./utils";
import * as types from "./types";

const log = logger("index");

export const Types = types;



export default class EsORM {
  constructor(config) {
    this.models = {};
    this.config = config;
  }
  async createClient() {
    log.debug(`creating client`, this.config);
    this.client = new Client(this.config);
  }
  async getClient() {
    if (!this.client) {
      await this.createClient();
    }
    return this.client;
  }
  async sync(options = {}) {

    const client = await this.getClient();
    return waterfall(Object.keys(this.models), async(modelName) => {
      const model = this.models[modelName];
      const indexName = model.indexName;
      const indexExists = await client.indices.exists({
        index: indexName,
      }).then(processElasticResponse);
      if (indexExists && options.force) {
        await client.indices.delete({
          index: indexName,
        }).then(processElasticResponse);
      }
      if (!indexExists || options.force) {
        await client.indices.create({
          index: indexName,
          include_type_name: true, //eslint-disable-line
          body: {
            mappings: {
              "_doc": {
                "properties": model.schema.mappings,
              },
            },
            settings: model.schema.settings,
          },
        }).then(processElasticResponse);
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
