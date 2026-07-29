declare module "elkjs/lib/elk-api.js" {
  import type {
    ELK,
    ELKConstructorArguments,
  } from "elkjs/lib/elk-api";

  const ElkConstructor: {
    new(args?: ELKConstructorArguments): ELK;
  };

  export default ElkConstructor;
}

declare module "elkjs/lib/elk-worker.min.js?worker" {
  const ElkWorker: {
    new(): Worker;
  };

  export default ElkWorker;
}
