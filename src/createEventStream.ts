import { IEventStream, IServiceStreamDefinition } from "atomservicescore";
import { Connector as RBMQConnector, IMQChannel } from "rbmq";
import { endpoints } from "./endpoints";

export const createEventStream = (configs?: { url: string; options?: any; }, factory?: any) => ((Configs): IEventStream => {
  let mqChannel: IMQChannel;
  const Factory = factory || RBMQConnector;
  const StreamDefinitions: IServiceStreamDefinition[] = [];

  const connector = Factory(Configs);
  const connect = async (): Promise<IMQChannel> => {
    if (mqChannel === undefined) {
      const cnn = await connector.connect();
      mqChannel = await cnn.createChannel();
    }

    return mqChannel;
  };

  return {
    connect: async () => {
      const Channel = await connect();
      const exchanges: string[] = [];
      const queues: { [q: string]: { q: string; onMessage: any; }; } = {};
      const binds: Array<{ ex: string; q: string; topic: string; }> = [];

      for (const definition of StreamDefinitions) {
        const { handlers, reactions, scope, type } = definition;

        handlers.events.forEach(({ name, level }) => {
          const ex = endpoints.toExchange({ level, scope, type });
          const q = endpoints.toQueue({ channel: "EventHandler", scope, type });

          if (exchanges.indexOf(ex) < 0) {
            exchanges.push(ex);
          }

          if (queues[q] === undefined) {
            const onMessage = Channel.toOnMessageWithAck((data, ack) => {
              const { event, metadata } = data;
              const processAck = async () => {
                ack();
              };
              handlers.processing(event, metadata, processAck);
            });

            queues[q] = { q, onMessage };
          }

          binds.push({ ex, q, topic: name });
        });

        reactions.events.forEach((reaction) => {
          const ex = endpoints.toExchange({ level: "Public", scope: reaction.scope, type: reaction.type });
          const q = endpoints.toQueue({ channel: "EventReaction", scope, type });

          if (exchanges.indexOf(ex) < 0) {
            exchanges.push(ex);
          }

          if (queues[q] === undefined) {
            const onMessage = Channel.toOnMessageWithAck((data, ack) => {
              const { event, metadata } = data;
              const processAck = async () => {
                ack();
              };
              reactions.processes[reaction.scope](event, metadata, processAck);
            });

            queues[q] = { q, onMessage };
          }

          binds.push({ ex, q, topic: reaction.name });
        });

        // 1. START :: assert Exchanges
        const assertExchanges = [];

        for (const ex of exchanges) {
          assertExchanges.push(Channel.assertExchange(ex, "direct", { autoDelete: true, durable: true }));
        }

        await Promise.all(assertExchanges);
        // END :: assert Exchanges

        // 2. START :: assert Queues
        const assertQueues = [];

        for (const q of Object.keys(queues)) {
          assertQueues.push(Channel.assertQueue(q, { autoDelete: true, durable: true }));
        }

        await Promise.all(assertExchanges);
        // END :: assert Queues

        // 3. START :: assert Binds
        const assertBinds = [];

        for (const { ex, q, topic } of binds) {
          assertBinds.push(Channel.bindQueue(q, ex, topic));
        }

        await Promise.all(assertBinds);
        // END :: assert Binds

        // 4. START :: assert Consumes
        const assertConsumes = [];

        for (const key of Object.keys(queues)) {
          const { q, onMessage } = queues[key];
          assertConsumes.push(Channel.consume(q, onMessage, { noAck: false }));
        }

        await Promise.all(assertConsumes);
        // END :: assert Consumes
      }
    },
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
    publish: async (on, metadata, event) => {
      const { level, scope } = on;
      const { name, type } = event;
      const Channel = await connect();

      const ex = endpoints.toExchange({ level, scope, type });
      const topic = name;
      const text = JSON.stringify({ event, metadata });
      const content = Buffer.from(text);
      await Channel.assertExchange(ex, "direct", { autoDelete: true, durable: true });
      await Channel.publish(ex, topic, content);
    },
    subscribe: async (definition) => {
      StreamDefinitions.push(definition);
    },
  };
})(configs);

Object.freeze(createEventStream);
