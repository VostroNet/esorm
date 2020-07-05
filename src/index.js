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
  createClient = async() => {
    log.debug(`creating client`, this.config);
    this.client = new Client(this.config);
  }
  getClient = async() => {
    if (!this.client) {
      await this.createClient();
    }
    return this.client;
  }
  sync = async(options = {}) => {

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
      if (model.schema.rollups) {
        await waterfall(Object.keys(model.schema.rollups).map(async(rollupName) => {
          const {body: {jobs}} = await client.rollup.getJobs({
            id: `${indexName}-${rollupName}`,
          });
          if (jobs.length === 0) {
            try {
              await client.rollup.putJob({
                id: rollupName,
                body: Object.assign({
                  index_pattern: `${indexName}*`, //eslint-disable-line
                  rollup_index: `${indexName}_rollup`, //eslint-disable-line
                }, model.schema.rollups[rollupName]),
              });
            } catch(err) {
              console.log("err", err);
            }
          }
        }));
      }
    });
  }
  define = (modelName, schema, options = {}) => {
    options.modelName = modelName;
    options.esorm = this;
    const model = class extends Model {};
    model.init(schema, options);
    this.models[modelName] = model;
    return model;
  }
  search = (query) => {
    const client = this.getClient();
    log.info("query", query);
    return client.search(query);
  }
}
