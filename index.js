/**
 * Extracts all attachments from a .mbox file.
 * This is designed to process HUGE mbox files; it was created to process an 80.6GB file extracted from GMail.
 *
 * Created by Rick Brown 2017-06-10.
 */
import {
	appendFile,
	appendFileSync,
	createWriteStream,
	existsSync,
	createReadStream,
	mkdirSync,
	renameSync,
	unlinkSync,
	writeFileSync,
} from "fs";
import { MailParser } from "mailparser";
// import { Mbox } from "node-mbox"; // v2
import Mbox from "node-mbox";
import path from "path";
import sanitize from "sanitize-filename";
import { pipeline } from "stream/promises";
import { once } from "events";
import { finished } from "stream/promises";
import { randomUUID } from "crypto";

const MAX_MEMORY_SIZE = 10 * 1024 * 1024;
const LOG_INFO_FILENAME = "info.log";
const LOG_ERROR_FILENAME = "error.log";
const LOG_ID_GLOBAL = randomUUID().slice(0, 4);
const NO_EXTENSION_FOLDERNAME = "_";
const GENERIC_EXTENSION = ".attachment";
const ALLOWED_EXTENSIONS = [
	"asc",
	"avi",
	"bmp",
	"csv",
	"dib",
	"doc",
	"docx",
	"gif",
	"ics",
	"jpeg",
	"jpg",
	"json",
	"mp4",
	"odt",
	"p7s",
	"pdf",
	"png",
	"pps",
	"ppt",
	"pptx",
	"tif",
	"tiff",
	"txt",
	"vcf",
	"xls",
	"xlsm",
	"xlsx",
	"xltx",
];
const QUARANTINE_EXTENSIONS = [
	// These always have the generic extension added (quarantine)
	"htm",
	"svg",
	"zip",
	"rar",
	"lnk",
	"url",
	"js",
	"hta",
	"ps1",
	"vbs",
	"cmd",
	"bat",
	"exe",
	"msi",
];
const FILTER_ONLY = []; // If empty, all attachments that are not forbidden will be extracted
const FILTER_FORBIDDEN = [
	"lnk",
	"url",
	"js",
	"hta",
	"ps1",
	"vbs",
	"cmd",
	"bat",
	"exe",
	"msi",
];
const EXTRACT_MISSING_EXTENSIONS = true; // attachments without filename or without extension
const EXTRACT_UNKNOWN_EXTENSIONS = true;
const SANITIZE_UNKNOWN_EXTENSIONS = true; // add generic extension to files with unknown extension
const SANITIZE_MISSING_EXTENSIONS = false; // add generic extension to files without extension
const ALLOW_PATTERN_MIGRATION = true; // if the extension config changed, this allows to rename existing files to match it.
const EXTENSION_GROUPS = {
	none: null,
	allowed: 1,
	quarantined: 2,
	missing: 3,
	unknown: 4,
	filtered: -1,
};

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
	const runState = {
		extractionID: randomUUID().slice(0, 4),
		summary: new Map(),
		pendingAttachments: new Set(),
		pendingParsers: new Set(),
		pendingLogWrite: Promise.resolve(),
		input: "stdin",
	};

	if (config.outputDir) {
		ensureDirectoryExistence(config.outputDir);
		mbox = instantiateMbox(
			config.outputDir,
			!!config.dryRun,
			!!config.subDirs,
			runState,
		);
		if (!config.mboxFile) {
			addToConsole(runState, "No mbox file provided. Waiting for stdin.");
		} else {
			runState.input = path.basename(config.mboxFile);
		}
		addLineToLog(
			config.outputDir,
			runState,
			"START",
			config.mboxFile || "stdin",
		);
		streamMbox(mbox, config.mboxFile, runState);
	} else {
		addToConsole(runState, "Must specify outputDir");
	}
}

function calcTotals(v, k, m) {
	if (v && typeof v == "object" && this instanceof Map) {
		for (const [key, value] of Object.entries(v)) {
			this.set(key, (this.get(key) || 0) + value);
		}
	}
}

function addToConsole(runState, ...data) {
	console.log(
		"🆔 " + runState.extractionID,
		"📧 " + runState.input + " ➡️ ",
		...data,
	);
}

async function printSummary(outputDir, runState) {
	while (
		runState.pendingAttachments.size > 0 ||
		runState.pendingParsers.size > 0
	) {
		await Promise.all([
			...runState.pendingAttachments,
			...runState.pendingParsers,
		]);
	}

	// totals

	const totals = new Map();
	runState.summary.forEach(calcTotals, totals);

	// print information

	addToConsole(runState, "Totals:", totals);
	addToConsole(runState, "Stats:", runState.summary);

	await addLineToLog(
		outputDir,
		runState,
		"STATS",
		JSON.stringify([...runState.summary]),
	);
	await addLineToLog(
		outputDir,
		runState,
		"FINISH",
		JSON.stringify(Object.fromEntries(totals)),
	);
}

function updateSummary(runState, key, result, fileSize, procInMemory) {
	key = key.toLowerCase();
	let entry = runState.summary.get(key);
	if (!entry) {
		entry = {
			proc: 0,
			skip: 0,
			fail: 0,
			big: 0,
			bytes: 0,
		};
		runState.summary.set(key, entry);
	}

	entry[result]++;
	entry.bytes += Number(fileSize) || 0;
	if (!procInMemory) entry.big += 1;
}

function getExtensionGroup(extension) {
	extension = extension.trim().toLowerCase();
	// apply filters
	if (FILTER_ONLY && FILTER_ONLY.length > 0) {
		// explicit "filter only" wins over "filter forbidden", but not over group restrictions
		// example:
		// 		"filter only" contains "pdf", but "pdf" is unknown (not in allowed or quarantine)
		//		if EXTRACT_UNKNOWN_EXTENSIONS is off, this filter will not overwrite it to prevent
		//		unintended filenames. Either add pdf to a list or change the unknown settings.
		if (!FILTER_ONLY.includes(extension.slice(1))) {
			return EXTENSION_GROUPS.filtered;
		}
	} else if (
		FILTER_FORBIDDEN &&
		FILTER_FORBIDDEN.includes(extension.slice(1))
	) {
		return EXTENSION_GROUPS.filtered;
	}

	// group extensions

	if (extension == NO_EXTENSION_FOLDERNAME) {
		return EXTRACT_MISSING_EXTENSIONS
			? EXTENSION_GROUPS.missing
			: EXTENSION_GROUPS.filtered;
	}
	if (extension == ".") {
		console.log(
			"Script error. Dot only should not be processed as extension.",
			extension,
		);
		return EXTRACT_MISSING_EXTENSIONS
			? EXTENSION_GROUPS.missing
			: EXTENSION_GROUPS.filtered;
	}
	if (extension.slice(0, 1) != ".") {
		console.log(
			"Script error. Invalid extension supplied for test (missing dot).",
			extension,
		);
		return EXTENSION_GROUPS.filtered;
	}
	if (QUARANTINE_EXTENSIONS.includes(extension.slice(1))) {
		return EXTENSION_GROUPS.quarantined;
	}
	if (ALLOWED_EXTENSIONS.includes(extension.slice(1))) {
		return EXTENSION_GROUPS.allowed;
	}
	return EXTRACT_UNKNOWN_EXTENSIONS
		? EXTENSION_GROUPS.unknown
		: EXTENSION_GROUPS.filtered;
}

/**
 * Creates an instance of Mbox ready to run.
 * @param {String} outputDir The path to the output directory.
 * @param {Boolean} dryRun If true do not write any files to the output directory.
 * @param {Boolean} subDirs If true create a sub directory for each day.
 * @returns {Mbox} An instance of node-mbox.
 */
function instantiateMbox(outputDir, dryRun, subDirs, runState) {
	var mbox = new Mbox();
	let messageNumber = 0;

	mbox.on("pipe", function () {
		addToConsole(runState, "start reading mbox file");
	});

	mbox.on("error", function (err) {
		addToConsole(runState, "error while reading mbox file", err);
	});

	mbox.on("finish", function () {
		addToConsole(runState, "finished reading mbox file");
		printSummary(outputDir, runState).catch((error) => {
			console.error("ERROR: Could not print summary", error);
		});
	});
	mbox.on("message", function (msg) {
		//"message" in node-mbox v1, is "data" on v2
		messageNumber++;
		var mailParser = new MailParser({
			streamAttachments: true,
			checksumAlgo: "md5", // md5, sha1, sha256, sha512, ...
		});
		const parserCompletion = new Promise((resolve) => {
			let completed = false;
			const complete = () => {
				if (!completed) {
					completed = true;
					resolve();
				}
			};
			mailParser.once("end", complete);
			mailParser.once("error", complete);
		});
		runState.pendingParsers.add(parserCompletion);
		parserCompletion.then(
			() => runState.pendingParsers.delete(parserCompletion),
			() => runState.pendingParsers.delete(parserCompletion),
		);
		let labelDate = "";
		let debugStepData = "";
		let attachmentNumber = 0;
		const messageInfo = {
			number: messageNumber,
			isBuffer: Buffer.isBuffer(msg),
			byteLength: msg?.length,
		};

		// Parser failures are emitted asynchronously, often while end() finalizes the message.
		mailParser.on("error", function (error) {
			console.error("ERROR: Parsing message", messageInfo);
			console.error(error);
			const errorLog = path.join(outputDir, LOG_ERROR_FILENAME);
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

		mailParser.on("data", async function (data) {
			debugStepData = data.filename;
			if (data.type === "attachment") {
				attachmentNumber++;
				const temporaryFile = path.join(
					outputDir,
					`.mboxtract-${messageNumber}-${attachmentNumber}.tmp`,
				);
				const rawExtension = path.extname(data.filename || "");
				const fileExtension =
					rawExtension === "."
						? NO_EXTENSION_FOLDERNAME
						: rawExtension || NO_EXTENSION_FOLDERNAME;
				const fileExtensionGroup = getExtensionGroup(fileExtension);

				if (fileExtensionGroup == EXTENSION_GROUPS.filtered) {
					updateSummary(
						runState,
						"filter|" + fileExtension,
						"skip",
						0,
						true,
					);
					addLineToLog(
						outputDir,
						runState,
						"FILTER",
						fileExtensionGroup,
						data.filename,
					);
					return data.release();
				}

				const attachmentProcessing = (async () => {
					try {
						// Consume the stream first; mailparser sets data.checksum when it ends.
						// await pipeline(
						// 	data.content,
						// 	createWriteStream(temporaryFile),
						// );

						const chunks = [];
						let bufferedBytes = 0;
						let temporaryStream;

						for await (const chunk of data.content) {
							if (
								!temporaryStream &&
								bufferedBytes + chunk.length <= MAX_MEMORY_SIZE
							) {
								chunks.push(chunk);
								bufferedBytes += chunk.length;
								continue;
							}

							if (!temporaryStream) {
								temporaryStream =
									createWriteStream(temporaryFile);

								for (const bufferedChunk of chunks) {
									if (!temporaryStream.write(bufferedChunk)) {
										await once(temporaryStream, "drain");
									}
								}

								chunks.length = 0;
							}

							if (!temporaryStream.write(chunk)) {
								await once(temporaryStream, "drain");
							}
						}

						let procInMemory = true;
						if (temporaryStream) {
							procInMemory = false;
							temporaryStream.end();
							await finished(temporaryStream);
						} else {
							// For small attachments that where completely read in memory
							// the data.checksum is calculated by MailParser without writing
							// a temp file. This allows to run the getUniquePath() checks before writing.
							// const attachmentBuffer = Buffer.concat(chunks);
						}

						const fileHash = data.checksum;
						const fileSize = data.size;
						const filename = data.filename
							? sanitize(data.filename)
							: fileHash;
						const fileToWrite = getUniquePath(
							outputDir,
							fileExtension,
							labelDate,
							filename,
							fileHash,
							fileExtensionGroup,
						);

						const key = `${labelDate}|${fileExtension}`;

						if (!dryRun && fileToWrite) {
							updateSummary(
								runState,
								key,
								"proc",
								fileSize,
								procInMemory,
							);
							addLineToLog(
								outputDir,
								runState,
								"PROC",
								fileHash,
								procInMemory,
								fileSize,
								filename,
								fileToWrite,
							);
							ensureDirectoryExistence(path.dirname(fileToWrite));
							if (procInMemory) {
								writeFileSync(
									fileToWrite,
									Buffer.concat(chunks),
								);
							} else {
								renameSync(temporaryFile, fileToWrite);
							}
						} else {
							updateSummary(
								runState,
								key,
								"skip",
								fileSize,
								procInMemory,
							);
							addLineToLog(
								outputDir,
								runState,
								"SKIP",
								fileHash,
								procInMemory,
								fileSize,
								filename,
								fileToWrite,
							);
							if (!procInMemory) {
								unlinkSync(temporaryFile);
							}
						}
					} catch (error) {
						const key = `${labelDate || "0000"}|${fileExtension}`;
						updateSummary(
							runState,
							key,
							"fail",
							data.size || 0,
							false,
						);
						console.error("ERROR: Attachment processing failed", {
							message: messageNumber,
							filename: data.filename,
						});
						console.error(error);
						if (existsSync(temporaryFile))
							unlinkSync(temporaryFile);
					} finally {
						data.release();
					}
				})();
				runState.pendingAttachments.add(attachmentProcessing);
				attachmentProcessing.then(
					() =>
						runState.pendingAttachments.delete(
							attachmentProcessing,
						),
					() =>
						runState.pendingAttachments.delete(
							attachmentProcessing,
						),
				);
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

async function addLineToLog(outputDir, runState, ...data) {
	const infoLog = path.join(outputDir, LOG_INFO_FILENAME);
	const marker = path.basename(path.dirname(infoLog));

	const parts = [].concat(
		new Date().toISOString(),
		LOG_ID_GLOBAL,
		runState.extractionID,
		marker,
		data,
	);

	const line = parts.join(" ") + "\n";
	runState.pendingLogWrite = runState.pendingLogWrite
		.catch(() => {})
		.then(
			() =>
				new Promise((resolve, reject) => {
					appendFile(infoLog, line, (error) => {
						if (error) reject(error);
						else resolve();
					});
				}),
		);

	try {
		await runState.pendingLogWrite;
	} catch (error) {
		console.error("Could not write log entry", error);
	}
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
function getUniquePath(
	currentDir,
	labelExt = "",
	labelDate = "",
	filename,
	hash = "",
	fileExtensionGroup,
) {
	const filepath = path.join(currentDir, sanitize(filename));
	const dir = path.dirname(filepath);
	const ext = path.extname(filepath);
	const base = path.basename(filepath, ext);

	const outputDir = path.join(dir, sanitize(labelExt), sanitize(labelDate));
	ensureDirectoryExistence(outputDir);

	// create custom extensions for files
	// const fileExtensionGroup = getExtensionGroup(ext);
	let customExt = "";
	switch (fileExtensionGroup) {
		case EXTENSION_GROUPS.quarantined:
			customExt = GENERIC_EXTENSION;
			break;
		case EXTENSION_GROUPS.allowed:
			break;
		case EXTENSION_GROUPS.unknown:
			if (SANITIZE_UNKNOWN_EXTENSIONS) customExt = GENERIC_EXTENSION;
			break;
		case EXTENSION_GROUPS.missing:
			if (SANITIZE_MISSING_EXTENSIONS) customExt = GENERIC_EXTENSION;
			break;

		default:
			customExt = GENERIC_EXTENSION;
			break;
	}
	const newExt = ext + customExt;
	const oldExt = customExt ? ext : ext + GENERIC_EXTENSION;

	let oldCandidate = path.join(outputDir, `${base}${oldExt}`);
	let newCandidate = path.join(outputDir, `${base}${newExt}`);

	if (hash.length > 0) {
		switch (true) {
			case hash == base:
				// variant 1: hash-only filenames
				oldCandidate = path.join(outputDir, `${base}${oldExt}`);
				newCandidate = path.join(outputDir, `${base}${newExt}`);
				break;

			case base.length == 0:
				// variant 1b: missing filenames (fallback to hash)
				oldCandidate = path.join(outputDir, `${hash}${oldExt}`);
				newCandidate = path.join(outputDir, `${hash}${newExt}`);
				break;

			default:
				oldCandidate = path.join(outputDir, `${base}_${hash}${oldExt}`);
				newCandidate = path.join(outputDir, `${base}_${hash}${newExt}`);
				break;
		}

		try {
			if (existsSync(newCandidate)) {
				// drop hashed duplicate
				return null;
			}
			if (existsSync(oldCandidate)) {
				// drop hashed duplicate
				if (ALLOW_PATTERN_MIGRATION) {
					renameSync(oldCandidate, newCandidate);
					console.log(
						"Group",
						fileExtensionGroup,
						"Migrated",
						oldCandidate,
						"to",
						newCandidate,
					);
				}
				return null;
			}
			return newCandidate;
		} catch (error) {
			console.error(error);
		}
	} else {
		let counter = 1;
		// there is no way to migrate anything without hashes because equal filenames tell
		// nothing about the content. Without hashes, this script is not idempotent and will
		// create infinite duplicates on re-runs.

		try {
			// variant 3: filename + running number for every duplicate
			while (existsSync(newCandidate)) {
				newCandidate = path.join(
					outputDir,
					`${base}_${counter}${newExt}`,
				);
				counter++;
			}
			return newCandidate;
		} catch (err) {
			console.error(err);
		}
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
function streamMbox(mbox, mboxFile, runState) {
	var mboxStream;
	if (!mboxFile) {
		mboxStream = process.stdin;
	} else if (existsSync(mboxFile)) {
		mboxStream = createReadStream(mboxFile);
	} else {
		addToConsole(runState, "Can't find your mbox file", mboxFile);
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
		mkdirSync(dirName, { recursive: true });
	}
}

// export const extract = extract;
