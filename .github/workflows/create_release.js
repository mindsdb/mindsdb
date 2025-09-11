/**
 * Este script:
 *  1 - cria uma release
 *  2 - para cada PR incluida na release, obtem o card do shortcut e coloca a label de "Status / Live"
 */

const axios = require('axios');

/**
 * @param github A pre-authenticated {@see https://octokit.github.io/rest.js} client with pagination plugins
 * @param context An object containing the context of the workflow run {@see https://github.com/actions/toolkit/blob/main/packages/github/src/context.ts}
 * @param core A reference to the @actions/core package
 * @return {Promise<void>}
 *
 * @see rest api docs https://docs.github.com/en/rest
 * @see action docs https://github.com/actions/github-script
 */
module.exports = async ({github, context}) => {
    const tagName = "v" + process.env.APP_VERSION
    const SHORTCUT_API_TOKEN = process.env.SHORTCUT_API_TOKEN

    const shortcutClient = axios.create({
        baseURL: 'https://api.app.shortcut.com/api/v3',
        timeout: 5000,
        headers: {'Shortcut-Token': SHORTCUT_API_TOKEN}
    });

    const refResponse = await github.rest.git.createRef({
        owner: context.repo.owner,
        repo: context.repo.repo,
        ref: "refs/tags/" + tagName,
        sha: context.sha,
    });
    const refData = refResponse.data;

    await github.rest.git.createTag({
        owner: context.repo.owner,
        repo: context.repo.repo,
        tag: tagName,
        message: "Version " + tagName,
        object: refData.object.sha,
        type: refData.object.type,
        tagger: {
            name: "Deploy Workflow", // context.payload.pusher.name, so tem pusher se for PR
            email: "tobias.ferreira@talentify.io", //context.payload.pusher.email,
        }
    });

    // Useful to test this script without creating a new release every time.
    // const {data: releaseData} = await github.rest.repos.getLatestRelease({
    //     owner: context.repo.owner,
    //     repo: context.repo.repo,
    // });
    const {data: releaseData} = await github.rest.repos.createRelease({
        owner: context.repo.owner,
        repo: context.repo.repo,
        tag_name: tagName,
        name: tagName,
        generate_release_notes: true,
    });

    const changes = releaseData.body.split("\n");
    for (change of changes) {
        console.group("Changes");
        // e.g.:
        // https://github.com/Talentify/jobs-search/pull/298 returns 298
        const matches = change.match(/github\.com\/(.*)\/(.*)\/pull\/([0-9]*)/);
        if (!matches || !matches[3]) {
            console.debug("No reference to a Pull Request was found on the current line.");
            console.groupEnd();
            continue;
        }
        const pullRequestNumber = matches[3];

        console.group("PR-" + pullRequestNumber);
        console.debug("Checking pull request", pullRequestNumber);

        const {data: comments} = await github.request('GET /repos/{owner}/{repo}/issues/{pull_number}/comments', {
            owner: context.repo.owner,
            repo: context.repo.repo,
            pull_number: pullRequestNumber,
        });

        if (!comments) {
            console.info("No comments found");
            console.groupEnd();
            console.groupEnd();
            continue;
        }

        for (comment of comments) {
            // e.g.: https://app.shortcut.com/talentify/story/37685/some-title
            const shortcurtUrlParts = comment.body.match(/app\.shortcut\.com\/(.*)\/story\/([0-9]*)/)
            if (!shortcurtUrlParts || !shortcurtUrlParts[2]) {
                console.debug("Comment does not contain a reference to the shortcut story.");
                console.groupEnd();
                console.groupEnd();
                continue;
            }

            const shortcutStoryId = shortcurtUrlParts[2];
            console.log("shortcutStoryId", shortcutStoryId);

            shortcutClient.put('/stories/' + shortcutStoryId, {
                labels: [
                    {
                        name: "Status / Live",
                    }
                ],
            }).catch(function (error) {
                console.log(error.toJSON());
            });
        }

        console.groupEnd();
        console.groupEnd();
    }
}

