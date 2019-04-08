import logger from "./logger";
const log = logger("utils");

export function processElasticResponse(response) {
  if (Array.isArray(response.warnings)) {
    response.warnings.forEach((warn) => {
      log.warn(warn);
    });
  }
  return response.body;
}
