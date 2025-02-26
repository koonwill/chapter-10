const express = require("express");
const mongodb = require("mongodb");
const amqp = require("amqplib");
const multer = require("multer"); // Handles image uploads
const path = require("path");
const fs = require("fs");

//
// Starts the microservice.
//
async function startMicroservice(dbHost, dbName, rabbitHost, port) {
    const client = await mongodb.MongoClient.connect(dbHost, { useUnifiedTopology: true });
    const db = client.db(dbName);
    const imageCollection = db.collection("images");

    const messagingConnection = await amqp.connect(rabbitHost);
    const messageChannel = await messagingConnection.createChannel();

    const app = express();
    app.use(express.json());

    // Configure Multer to store uploaded images
    const uploadDir = "./uploads"; // Ensure this directory exists
    if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir);

    const storage = multer.diskStorage({
        destination: (req, file, cb) => cb(null, uploadDir),
        filename: (req, file, cb) => cb(null, Date.now() + path.extname(file.originalname)),
    });

    const upload = multer({ storage });

    //
    // HTTP GET route to retrieve all images.
    //
    app.get("/images", async (req, res) => {
        const images = await imageCollection.find().toArray();
        res.json({ images });
    });

    //
    // HTTP GET route to retrieve details for a particular image.
    //
    app.get("/image", async (req, res) => {
        const imageId = new mongodb.ObjectId(req.query.id);
        const image = await imageCollection.findOne({ _id: imageId });
        if (!image) {
            res.sendStatus(404);
        } else {
            res.json({ image });
        }
    });

    //
    // HTTP POST route to upload an image.
    //
    app.post("/upload", upload.single("image"), async (req, res) => {
        if (!req.file) {
            return res.status(400).json({ error: "No image uploaded" });
        }

        const filenameWithoutExtension = path.basename(req.file.originalname, path.extname(req.file.filename));
        // Create the URL in the desired format: "www.{filename}.com"
        const url = `https://www.${filenameWithoutExtension}.com`;
        const imageUrl = `advertise/uploads/${req.file.filename}`;

        const newImage = { imageUrl, uploadedAt: new Date(), url };
        const result = await imageCollection.insertOne(newImage);

        res.status(201).json({ message: "Image uploaded", image: newImage });

        // Send event to RabbitMQ
        const message = { imageId: result.insertedId, imageUrl, url };
        messageChannel.sendToQueue("image-uploaded", Buffer.from(JSON.stringify(message)));
    });

    //
    // Handles incoming RabbitMQ messages.
    //
    async function consumeImageUploadedMessage(msg) {
        console.log("Received 'image-uploaded' message");

        const parsedMsg = JSON.parse(msg.content.toString());

        const imageMetadata = {
            _id: new mongodb.ObjectId(parsedMsg.imageId),
            imageUrl: parsedMsg.imageUrl,
            url: parsedMsg.url
        };

        await imageCollection.insertOne(imageMetadata);

        console.log("Acknowledging image-uploaded message.");
        messageChannel.ack(msg);
    }

    await messageChannel.assertExchange("image-uploaded", "fanout");

    const { queue } = await messageChannel.assertQueue("", {});
    await messageChannel.bindQueue(queue, "image-uploaded", "");
    await messageChannel.consume(queue, consumeImageUploadedMessage);

    app.listen(port, () => {
        console.log(`Advertise Microservice running on port: ${port}`);
    });
}

//
// Application entry point.
//
async function main() {
    if (!process.env.PORT) throw new Error("Please specify the port number using PORT.");
    if (!process.env.DBHOST) throw new Error("Please specify the database host using DBHOST.");
    if (!process.env.DBNAME) throw new Error("Please specify the database name using DBNAME.");
    if (!process.env.RABBIT) throw new Error("Please specify RabbitMQ host using RABBIT.");

    const PORT = process.env.PORT;
    const DBHOST = process.env.DBHOST;
    const DBNAME = process.env.DBNAME;
    const RABBIT = process.env.RABBIT;

    await startMicroservice(DBHOST, DBNAME, RABBIT, PORT);
}

if (require.main === module) {
    main().catch(err => {
        console.error("Advertise Microservice failed to start.");
        console.error(err && err.stack || err);
    });
} else {
    module.exports = { startMicroservice };
}
