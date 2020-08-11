import EsORM, {Types} from "../src/index";


import moment from "moment";
import faker from "faker";

function getRange(startDate, endDate, type) {
  let fromDate = moment(startDate);
  let toDate = moment(endDate);
  let diff = toDate.diff(fromDate, type);
  let range = [];
  for (let i = 0; i < diff; i++) {
    range.push(moment(startDate).add(i, type));
  }
  return range;
}


const testModelSchema = {
  defaultSearchIndex: "indexName",
  rotateIndex: true,
  rollups: {
    indexName: {
      perMinuteInterval: {
        cron: "0 * * * * ?",
        page_size: 1000, //eslint-disable-line
        groups: {
          "date_histogram": {
            "field": "time",
            "fixed_interval": "2m",
            "delay": "1m",
          },
          "terms": {
            "fields": ["manufacturer"],
          },
        },
        metrics: [{
          "field": "input",
          "metrics": ["sum"],
        }, {
          "field": "output",
          "metrics": ["sum"],
        }],
      },
      perDayInterval: {
        cron: "0 * * * * ?",
        page_size: 1000, //eslint-disable-line
        groups: {
          "date_histogram": {
            "field": "time",
            "fixed_interval": "1h",
            "delay": "1d",
          },
          "terms": {
            "fields": ["manufacturer"],
          },
        },
        metrics: [{
          "field": "input",
          "metrics": ["max"],
        }, {
          "field": "output",
          "metrics": ["max"],
        }],
      },
    },
  },
  mappings: {
    "time": {
      type: Types.date,
    },
    "input": {
      type: Types.Long,
    },
    "output": {
      type: Types.Long,
    },
    "manufacturer": {
      type: Types.Keyword,
    },
  },
};

const nodeConfig = {
  nodes: ["http://10.97.1.100:9200"],
};
jest.useFakeTimers();
test("rollup test", async() => {
  try {
    const instance = new EsORM(nodeConfig);
    instance.define("TestModelRoll", testModelSchema, {});
    await instance.sync({force: true});
  } catch(err) {
    console.log("err", err);
    expect(1).toBe(0);
  }
  expect(1).toBe(1);
});


test("rollup query", async() => {
  try {
    const instance = new EsORM(nodeConfig);
    instance.define("TestModelRoll", testModelSchema, {});
    await instance.sync({force: true});
    const {TestModelRoll} = instance.models;

    const range = getRange(moment().add(-1, "hour"), moment().add(-0.5, "hour"), "minute");
    await TestModelRoll.createBulk(range.reduce((o, v) => {
      for (var i = 0; i < 20; i++) {
        o.push({
          "time": v.toISOString(),
          "input": Math.round(Math.random() * 100),
          "output": Math.round(Math.random() * 100),
          "manufacturer": faker.name.firstName(),
        });
      }
      return o;
    }, []));
    const results = await TestModelRoll.query({
      "aggregations": {
        "max_input": {
          "max": {
            "field": "input",
          },
        },
      },
    });
    console.log("results", results);

  } catch(err) {
    console.log("err", err);
    expect(1).toBe(0);
  }
  expect(1).toBe(1);
});
