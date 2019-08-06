import { IEventStream } from "atomservicescore";
import { Connector, IMQChannel } from "rbmq";
import { endpoints } from "./endpoints";

export const createEventStream = (configs?: { url: string; options?: any; }) => ((Configs): IEventStream => {
  let mqChannel: IMQChannel;

  const connector = Connector(Configs);
  const connect = async (): Promise<IMQChannel> => {
    if (mqChannel === undefined) {
      const cnn = await connector.connect();
      mqChannel = await cnn.createChannel();
    }

    return mqChannel;
  };

  return {
    directTo: async (ref, data) => {
      const channel = await connect();

      const q = ref.toString();
      const text = typeof data === "string" ? data : JSON.stringify(data);

      await channel.sendToQueue(q, Buffer.from(text));
    },
    listenTo: async (ref, listeners) => {
      const channel = await connect();

      const q = ref.toString();
      await channel.assertQueue(q, { autoDelete: true, durable: true });

      await channel.consume(q, channel.toOnMessage(listeners), { noAck: true });
    },
    publish: async (event, on, metadata) => {
      const channel = await connect();

      const ex = endpoints.toExchange({ level: on.level, scope: on.scope, type: event.type });
      const topic = event.name;
      const text = JSON.stringify(event);
      const content = Buffer.from(text);
      await channel.assertExchange(ex, "direct", { autoDelete: true, durable: true });

      await channel.publish(ex, topic, content);
    },
    subscribe: async (on, to, process) => {
      const channel = await connect();

      const ex = endpoints.toExchange(on);
      const q = endpoints.toQueue(to);
      const topic = on.name;
      await channel.assertExchange(ex, "direct", { autoDelete: true, durable: true });
      await channel.assertQueue(q, { autoDelete: true, durable: true });
      await channel.bindQueue(q, ex, topic);

      await channel.consume(q, channel.toOnMessageWithAck(process), { noAck: false });
    },
  };
})(configs);

Object.freeze(createEventStream);
