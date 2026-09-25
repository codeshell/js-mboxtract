#!/usr/bin/env node
import { access, mkdir, rename, writeFile } from "fs/promises";
import { dirname, join, resolve } from "path";
import { fileURLToPath } from "url";
import { extract } from "../index.js";


// https://stackoverflow.com/questions/11171837/large-sample-mbox-file-for-testing-purposes
// https://www.cs.cmu.edu/~enron/
// https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz
// I am distributing this dataset as a resource for researchers who are interested in improving current email tools, or understanding how email is currently used. This data is valuable; to my knowledge it is the only substantial collection of "real" email that is public. The reason other datasets are not public is because of privacy concerns. In using this dataset, please be sensitive to the privacy of the people involved (and remember that many of these people were certainly not involved in any of the actions which precipitated the investigation.)
// maildir, need https://github.com/lintool/Enron2mbox

// https://opendata.stackexchange.com/questions/4517/obtaining-personal-mail-corpus
// https://web.archive.org/web/20160408030221/https://ab21www.s3.amazonaws.com/JebBushEmails-Text.7z
// no mbox, only redacted text, cannot use

// From https://lists.apache.org/list?dev@perl.apache.org:2000-9
// https://lists.apache.org/api/mbox.lua?list=dev&domain=perl.apache.org&d=2000-9

const fixtureNames = [
	"base64-1",
	"base64-2",
	"basic1",
	"bug505221",
	"bugmail11",
	"charsets",
	"message-encoded",
	"mime-torture",
	"multipart-base64-1",
	"multipart-base64-2",
	"multipart-base64-3",
	"multipart-complex1",
	"multipart-complex2",
	"multipart1",
	"multipart2",
	"multipart3",
	"multipart4",
	"multipartmalt-detach",
	"shift-jis-image",
];
const testDirectory = dirname(fileURLToPath(import.meta.url));
const downloadDirectory = resolve(testDirectory, "downloads");
const fixtureBaseUrl =
	"https://hg-edge.mozilla.org/comm-central/raw-file/tip/mailnews/mime/jsmime/test/unit/data";

async function ensureFixture(fixtureName) {
	const fixturePath = join(downloadDirectory, fixtureName);
	await mkdir(downloadDirectory, { recursive: true });

	try {
		await access(fixturePath);
		return fixturePath;
	} catch {
		const fixtureUrl = `${fixtureBaseUrl}/${fixtureName}`;
		console.log(`Downloading ${fixtureUrl}`);
		const response = await fetch(fixtureUrl);
		if (!response.ok) {
			throw new Error(
				`Could not download ${fixtureName}: ${response.status} ${response.statusText}`,
			);
		}

		const temporaryPath = `${fixturePath}.tmp`;
		await writeFile(
			temporaryPath,
			Buffer.from(await response.arrayBuffer()),
		);
		await rename(temporaryPath, fixturePath);
	}

	return fixturePath;
}

for (const fixtureName of fixtureNames) {
	const fixturePath = await ensureFixture(fixtureName);
	extract({
		// using a static folder for all testfiles instead of fixtureName allows to keep
		// all results in one info.log
		outputDir: join("logs", "test_downloads"),
		subDirs: true,
		mboxFile: fixturePath,
	});
}
