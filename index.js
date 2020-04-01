const fs = require('fs');
const { ServiceBroker } = require("moleculer");

const broker1 = new ServiceBroker({
    nodeID: 'node1',
    transporter: 'NATS',
    options: {
        url: 'nats://localhost:4222',
    },
    disableBalancer: true,
    serializer: 'Notepack',
});

const broker2 = new ServiceBroker({
    nodeID: 'node2',
    transporter: 'NATS',
    options: {
        url: 'nats://localhost:4222',
    },
    disableBalancer: true,
    serializer: 'Notepack',
});

broker1.createService({
    name: 'fileReader',
    actions: {
        async getFileContents(ctx) {
            const { filePath } = ctx.params;
            const stream = fs.createReadStream(filePath);
            // return ctx.call('fileReader.readStreamWithDataEvents', stream); // This one never returns
            // return ctx.call('fileReader.readStreamWithReadableEvent', stream); // This one calls readStreamWithReadableEvent twice

            return ctx.call('streamReader.readStreamWithDataEvents', stream); // This one works fine
            // return ctx.call('streamReader.readStreamWithReadableEvent', stream); // This one works fine
        },
        async readStreamWithDataEvents(ctx) {
            console.log('called fileReader.readStreamWithDataEvents');
            const result = await new Promise(resolve => {
                const chunks = [];
                ctx.params.on('data', chunk => {
                    chunks.push(chunk);
                });
                ctx.params.on('end', () => {
                    resolve(Buffer.concat(chunks));
                });
            })
            return result.toString();
        },
        async readStreamWithReadableEvent(ctx) {
            console.log('called fileReader.readStreamWithReadableEvent');
            const result = await new Promise(resolve => {
                ctx.params.on('readable', () => {
                    const chunks = [];
                    let chunk = ctx.params.read();
                    while (chunk != null) {
                        chunks.push(chunk);
                        chunk = ctx.params.read();
                    }
                    resolve(Buffer.concat(chunks));
                });
            });
            return result.toString();
        },
    }
});

broker2.createService({
    name: 'streamReader',
    actions: {
        async readStreamWithDataEvents(ctx) {
            console.log('called streamReader.readStreamWithDataEvents');
            const result = await new Promise(resolve => {
                const chunks = [];
                ctx.params.on('data', chunk => {
                    chunks.push(chunk);
                });
                ctx.params.on('end', () => {
                    resolve(Buffer.concat(chunks));
                });
            })
            return result.toString();
        },
        async readStreamWithReadableEvent(ctx) {
            console.log('called streamReader.readStreamWithReadableEvent');
            const result = await new Promise(resolve => {
                ctx.params.on('readable', () => {
                    const chunks = [];
                    let chunk = ctx.params.read();
                    while (chunk != null) {
                        chunks.push(chunk);
                        chunk = ctx.params.read();
                    }
                    resolve(Buffer.concat(chunks));
                });
            });
            return result.toString();
        },
    }
})

broker1.start()
    .then(() => broker2.start())
    .then(() => broker1.call("fileReader.getFileContents", { filePath: './test.txt' }))
    .then(res => console.log("Read file contents:", res))
    .catch(err => console.error(`Error occurred! ${err.message}`));