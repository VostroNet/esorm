import EsORM, {Types} from "../src/index";

const testModelSchema = {
  rollups: {
    day: {
      cron: "0 8 * * *",
      page_size: 1000, //eslint-disable-line
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

test("rollup", async() => {
  const instance = new EsORM(nodeConfig);
  instance.define("TestModel", testModelSchema, {});
  await instance.sync();

  expect(1).toBe(0);
});
