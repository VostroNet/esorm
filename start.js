require("@babel/register");
const {Types} = require("./src/index");
const EsORM = require("./src/index").default;

const testModelSchema = {
  rollups: {
    day: {
      cron: "0 8 * * *",
      groups: {
        "date_histogram": {
          "field": "timestamp",
          "fixed_interval": "1h",
          "delay": "7d",
        },
        "terms": {
          "fields": ["type"],
        },
      },
    },
  },
  mappings: {
    name: {
      type: Types.Text,
    },
  },
};

const nodeConfig = {
  nodes: ["http://localhost:9200"],
};

(async() => {
  const instance = new EsORM(nodeConfig);
  instance.define("TestModel", testModelSchema, {});
  await instance.sync();

})();
