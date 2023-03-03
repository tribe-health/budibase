import { lookpath } from "lookpath"
import fs from "fs"
import * as makeFiles from "./makeFiles"
import { logErrorToFile, downloadFile, error } from "../utils"
import yaml from "yaml"
import { DockerCompose } from "./types"

const ERROR_FILE = "docker-error.log"
const COMPOSE_URL =
  "https://raw.githubusercontent.com/Budibase/budibase/master/hosting/docker-compose.yaml"

export async function downloadDockerCompose() {
  const fileName = COMPOSE_URL.split("/").slice(-1)[0]
  try {
    await downloadFile(COMPOSE_URL, `./${fileName}`)
  } catch (err) {
    console.error(error(`Failed to retrieve compose file - ${err}`))
  }
}

export async function checkDockerConfigured() {
  const error =
    "docker/docker-compose has not been installed, please follow instructions at: https://docs.budibase.com/docs/docker-compose"
  const docker = await lookpath("docker")
  const compose = await lookpath("docker-compose")
  if (!docker || !compose) {
    throw error
  }
}

export function checkInitComplete() {
  if (
    !fs.existsSync(makeFiles.ENV_PATH) &&
    !fs.existsSync(makeFiles.COMPOSE_PATH)
  ) {
    throw "Please run the hosting --init command before any other hosting command."
  }
}

export async function handleError(func: Function) {
  try {
    await func()
  } catch (err: any) {
    if (err && err.err) {
      logErrorToFile(ERROR_FILE, err.err)
    }
    throw `Failed to start - logs written to file: ${ERROR_FILE}`
  }
}

export function getServices(path: string) {
  const dockerYaml = fs.readFileSync(path, "utf8")
  const parsedYaml = yaml.parse(dockerYaml)
  return { yaml: parsedYaml, services: parsedYaml.services }
}

export function getAppService(path: string) {
  const { yaml, services } = getServices(path),
    serviceList = Object.keys(services)
  let service
  if (services["app-service"]) {
    service = services["app-service"]
  } else if (serviceList.length === 1) {
    service = services[serviceList[0]]
  }
  return { yaml, service }
}

export function updateDockerComposeService(
  updateFn: (service: DockerCompose) => void
) {
  const opts = ["docker-compose.yaml", "docker-compose.yml"]
  const dockerFilePath = opts.find(name => fs.existsSync(name))
  if (!dockerFilePath) {
    console.log(error("Unable to locate docker-compose YAML."))
    return
  }
  const { yaml: parsedYaml, service } = getAppService(dockerFilePath)
  if (!service) {
    console.log(
      error(
        "Unable to locate service within compose file, is it a valid Budibase configuration?"
      )
    )
    return
  }
  updateFn(service)
  fs.writeFileSync(dockerFilePath, yaml.stringify(parsedYaml))
}
