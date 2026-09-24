/**
 * Extracts all attachments from a .mbox file.
 * This is designed to process HUGE mbox files; it was created to process an 80.6GB file extracted from GMail.
 *
 * Created by Rick Brown 2017-06-10.
 */
import {
	appendFileSync,
	createWriteStream,
	existsSync,
	createReadStream,
	mkdirSync,
} from "fs";
import { MailParser } from "mailparser";
// import { Mbox } from "node-mbox"; // v2
import Mbox from "node-mbox";
import path from "path";
import sanitize from "sanitize-filename";

/**
 * Extracts attachments from mbox.
 * @param config An object with properties as shown below:
 * @param {String} config.outputDir The path to the output directory.
 * @param {Boolean} [config.dryRun] If true do not write any files to the output directory.
 * @param {Boolean} [config.subDirs] If true create a sub directory for each day.
 * @param {String} [config.mboxFile] The path to the mbox file. If not provided you must pipe the mbox on stdin.
 */
export function extract(config) {
	var mbox;
	if (config.outputDir) {
		ensureDirectoryExistence(config.outputDir);
		mbox = instantiateMbox(
			config.outputDir,
			!!config.dryRun,
			!!config.subDirs,
		);
		if (!config.mboxFile) {
			console.log("No mbox file provided. Waiting for stdin.");
		}
		streamMbox(mbox, config.mboxFile);
	} else {
		console.log("Must specify outputDir");
	}
}

/**
 * Creates an instance of Mbox ready to run.
 * @param {String} outputDir The path to the output directory.
 * @param {Boolean} dryRun If true do not write any files to the output directory.
 * @param {Boolean} subDirs If true create a sub directory for each day.
 * @returns {Mbox} An instance of node-mbox.
 */
function instantiateMbox(outputDir, dryRun, subDirs) {
	var mbox = new Mbox();
	let messageNumber = 0;
	// Next, catch events generated:
	// mbox.on("data", function (msg) {
	// 	// `msg` is a `Buffer` instance
	// 	console.log("got a message", typeof msg);
	// 	// console.log("got a message", msg.toString().slice(0, 10));
	// });

	mbox.on("error", function (err) {
		console.log("got an error", err);
	});

	mbox.on("finish", function () {
		console.log("done reading mbox file");
	});
	mbox.on("message", function (msg) {
		//"message" in node-mbox v1, is "data" on v2
		messageNumber++;
		var mailParser = new MailParser({
			streamAttachments: true,
			checksumAlgo: "md5", // md5, sha1, sha256, sha512, ...
		});
		let labelDate = "";
		let debugStepData = "";
		const messageInfo = {
			number: messageNumber,
			isBuffer: Buffer.isBuffer(msg),
			byteLength: msg?.length,
		};

		// Parser failures are emitted asynchronously, often while end() finalizes the message.
		mailParser.on("error", function (error) {
			console.error("ERROR: Parsing message", messageInfo);
			console.error(error);
			const errorLog = path.join(outputDir, "error.log");
			const errorHeader = `\n\n--- mbox message ${messageNumber} (${messageInfo.byteLength} bytes) ---\n${error.stack || error}\n`;
			try {
				appendFileSync(errorLog, errorHeader);
				appendFileSync(errorLog, msg);
				appendFileSync(errorLog, "\n--- end mbox message ---\n");
			} catch (logError) {
				console.error(
					"ERROR: Could not write parser input to",
					errorLog,
				);
				console.error(logError);
			}
		});

		if (subDirs) {
			mailParser.on("headers", function (headers) {
				debugStepData = headers;
				var dirName,
					mailDate,
					headerDate = headers.get("date");
				if (headerDate) {
					try {
						mailDate = new Date(headerDate); // converting to date should adjust for locale
						dirName = [
							mailDate.getFullYear(),
							pad(mailDate.getMonth() + 1),
							pad(mailDate.getDate()),
						];
						dirName = dirName.join("-");
						labelDate = mailDate.getFullYear().toString();
						// currentDir = join(outputDir, dirName);
						// ensureDirectoryExistence(currentDir);
					} catch (ex) {
						console.error("Could not parse date ", headerDate);
					}
				}
				// console.log(headers.get("date"));
			});
		}

		mailParser.on("data", function (data) {
			debugStepData = data.filename;
			var myFile, fileToWrite;
			if (data.type === "attachment") {
				var fileHash = data.checksum;
				var filename = data.filename
					? sanitize(data.filename)
					: fileHash;
				fileToWrite = getUniquePath(
					outputDir,
					labelDate,
					filename,
					fileHash,
				);
				if (!dryRun && fileToWrite) {
					console.log("PROC: ", fileHash, filename, fileToWrite);
					myFile = createWriteStream(fileToWrite);
					data.content.pipe(myFile);
				} else {
					console.log("SKIP: ", fileHash, filename, fileToWrite);
				}
				data.release();
			}
		});
		try {
			mailParser.write(msg);
			mailParser.end();
		} catch (error) {
			console.error("ERROR: Synchronous parser failure", messageInfo);
			console.error(error);
		}
	});
	return mbox;
}

/**
 * Ensures that unrelated attachments with identical names but different content cannot overwrite each other.
 * Either by using hashes or by enumerating duplicates.
 * Can return null if a file with the same content (hash) already exists.
 * Can return null if an error occurred while checking for existing files.
 * @param {string} currentDir Export path
 * @param {string} labelDate Optional date label
 * @param {string} filename Suggested filename
 * @param {string} hash Optional hash value of file content
 * @returns string | null
 */
function getUniquePath(currentDir, labelDate = "", filename, hash = "") {
	const filepath = path.join(currentDir, filename);
	const dir = path.dirname(filepath);
	const ext = path.extname(filepath);
	const base = path.basename(filepath, ext);

	const outputDir = path.join(dir, sanitize(ext ?? "_"), labelDate);
	ensureDirectoryExistence(outputDir);

	let candidate = path.join(outputDir, `${base}${ext}`);
	let counter = 1;

	// variant 1: hash-only filenames
	if (hash.length > 0 && hash == base) return candidate;
	// variant 1b: missing filenames (fallback to hash)
	if (hash.length > 0 && base.length == 0)
		return path.join(outputDir, `${hash}${ext}`);

	try {
		if (existsSync(path)) {
			// variant 2: filename + optional hash if duplicate detected
			if (hash.length > 0)
				return path.join(outputDir, `${base}_${hash}${ext}`);
			// variant 3: filename + running number for every duplicate
			while (existsSync(candidate)) {
				candidate = path.join(outputDir, `${base}_${counter}${ext}`);
				counter++;
			}
		}
		return candidate;
	} catch (err) {
		console.error(err);
	}

	return null;
}

// Function to generate a hash from a string
// The algorithm used can be specified, or will default to SHA-512
// https://developer.mozilla.org/en-US/docs/Web/API/Web_Crypto_API/Non-cryptographic_uses_of_subtle_crypto#hash_tables_with_sha
// "SHA-1" (but don't use this in cryptographic applications)
// "SHA-256"
// "SHA-384"
// "SHA-512".
async function generateHash(str, algorithm = "SHA-512") {
	// Create an array buffer for the supplied string - this buffer contains an integer representation of the string which can be used to generate the hash
	let strBuffer = new TextEncoder().encode(str);

	// use SubtleCrypto to generate the hash using the specified algorithm
	const hash = await crypto.subtle.digest(algorithm, strBuffer);
	// The resulting hash is an arrayBuffer, and should be converted to its hexadecimal representation
	// Initialize the result as an empty string - the hexadecimal characters for the values in the array buffer will be appended to it
	let result = "";
	// The DataView view provides an interface for reading number types from the ArrayBuffer
	const view = new DataView(hash);
	// Iterate over each value in the arrayBuffer and append the converted hexadecimal value to the result
	for (let i = 0; i < hash.byteLength; i += 4) {
		result += ("00000000" + view.getUint32(i).toString(16)).slice(-8);
	}
	return result;
}

function pad(num) {
	var result = "0" + num;
	return result.slice(-2);
}

/**
 * Once the event listeners are ready to go, let's start piping an mbox.
 * @param {Mbox} mbox An instance of node-mbox.
 * @param {String} [mboxFile] The path to the mbox file. If not provided you must pipe the mbox on stdin.
 */
function streamMbox(mbox, mboxFile) {
	var mboxStream;
	if (!mboxFile) {
		mboxStream = process.stdin;
	} else if (existsSync(mboxFile)) {
		mboxStream = createReadStream(mboxFile);
	} else {
		console.log("Can't find your mbox file", mboxFile);
		return;
	}
	mboxStream.pipe(mbox);
}

/**
 * Ensures the directory exists and creates it if it doesn't.
 * @param dirName The path to the directory.
 */
function ensureDirectoryExistence(dirName) {
	if (!existsSync(dirName)) {
		mkdirSync(dirName, {recursive: true});
	}
}

// export const extract = extract;
