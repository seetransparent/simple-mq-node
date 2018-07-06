const fs = require('fs');
const assert = require('assert');

const fileCommitMessage = process.argv[2];
assert(fileCommitMessage, 'Commit message file not found');
const commitMessage = fs.readFileSync(fileCommitMessage, 'utf8');
assert(commitMessage, 'Commit message not found');
if (/^(Merge|HOTFIX|[A-Z]{1,6}-[0-9]{1,6})/.test(commitMessage)) {
  process.exit(0);
} else {
  console.log(`Invalid commit message <${commitMessage.trim()}>. Please remember to add the issue number or "HOTFIX" or "Merge" as head of the message`);
  process.exit(1);
}
