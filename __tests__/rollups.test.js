import EsORM, {Types} from "../src/index";

const testModelSchema = {
  rollups: {
    day: {
      cron: "0 8 * * *",
      groups: {
        date_histogram: { //eslint-disable-line
          calendar_interval: "60m", //eslint-disable-line
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
