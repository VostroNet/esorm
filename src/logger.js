import debug from "debug";

const packageName = "inflx";

export default function(prefix = "") {
  let err = debug(`${packageName}:${prefix}:error`);
  return {
    err,
    error: err,
    info: debug(`${packageName}:${prefix}:info`),
    warn: debug(`${packageName}:${prefix}:warn`),
    debug: debug(`${packageName}:${prefix}:debug`),
  };
}
