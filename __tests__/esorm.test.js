import EsORM, {Types} from "../src/index";
console.log("EsORM", EsORM);

const testModelSchema = {
  fields: {
    name: Types.text,
  }
};


test("basic", async() => {
  const instance = new EsORM({
    hosts: ["http://localhost:9200"]
  });
  instance.define("TestModel", testModelSchema, {});
  await instance.sync();

  expect(1).toBe(1);
});
