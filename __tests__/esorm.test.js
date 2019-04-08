import EsORM, {Types} from "../src/index";

const testModelSchema = {
  mappings: {
    name: {
      type: Types.text,
    },
  },
};
const nodeConfig = {
  nodes: ["http://localhost:9200"]
};

test("basic sync", async() => {
  const instance = new EsORM(nodeConfig);
  instance.define("TestModel", testModelSchema, {});
  await instance.sync();

  expect(1).toBe(1);
});

test("create bulk", async() => {
  const instance = new EsORM(nodeConfig);
  instance.define("TestModel", testModelSchema, {});
  await instance.sync({force: true});
  const {TestModel} = instance.models;

  const indexes = await TestModel.createBulk([
    {name: "obiwan"},
    {name: "yoda"},
    {name: "old ben"},
  ]);

  expect(indexes.length).toBe(3);
});

test("count", async() => {
  const instance = new EsORM(nodeConfig);
  instance.define("TestModel", testModelSchema, {});
  await instance.sync({force: true});
  const {TestModel} = instance.models;

  await TestModel.createBulk([
    {name: "obiwan"},
    {name: "yoda"},
    {name: "old ben"},
  ]);
  const fullCount = await TestModel.count();

  expect(fullCount).toBe(3);
});


test("findAll", async() => {
  const instance = new EsORM(nodeConfig);
  instance.define("TestModel", testModelSchema, {});
  await instance.sync({force: true});
  const {TestModel} = instance.models;

  await TestModel.createBulk([
    {name: "obiwan"},
    {name: "yoda"},
    {name: "old ben"},
  ]);
  const results = await TestModel.findAll({
    query: {
      "term": {
        "name": "obiwan",
      },
    },
  });

  expect(results.models.length).toBe(1);
});
