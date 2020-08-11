import {Client} from "@elastic/elasticsearch";
import logger from "./logger";
import Model from "./model";
import waterfall from "./waterfall";
import * as types from "./types";
import { processElasticResponse } from "./utils";

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
    return waterfall(Object.keys(this.models), async(modelName) => this.models[modelName].sync(options));
  }
  findIndex = async(search) => {
    const client = await this.getClient();
    try {
      const results = await client.cat.indices({
        index: search,
        h: "i",
      }).then(processElasticResponse);
      return results.split("\n").filter((f) => f !== "" && !(!f));
    } catch (err) {
      if (err.statusCode === 404) {
        return [];
      }
      throw err;
    }
  }

  define = (modelName, schema, options = {}) => {
    options.modelName = modelName;
    options.esorm = this;
    const model = class extends Model {};
    model.init(schema, options);
    this.models[modelName] = model;
    return model;
  }
  search = async(query) => {
    const client = await this.getClient();
    log.info("query", query);
    return client.search(query);
  }
}
