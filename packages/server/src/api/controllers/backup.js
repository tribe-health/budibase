const { streamBackup } = require("../../utilities/fileSystem")
const { objectStore } = require("@budibase/backend-core")

exports.exportAppDump = async function (ctx) {
  const { appId } = ctx.query
  const appName = decodeURI(ctx.query.appname)
  const backupIdentifier = `${appName}-export-${new Date().getTime()}.txt`
  ctx.attachment(backupIdentifier)
  ctx.body = await streamBackup(appId)

  const test = await objectStore.retrieve(
    "prod-budi-app-assets",
    "app_default_d5cfa5eba37c4ce8af11bb94d6d6e904/attachments/movie.mov"
  )
  console.log(test)
}
