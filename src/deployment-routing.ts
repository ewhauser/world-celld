/** Operator-owned immutable callback destinations, keyed by stored deployment ID. */
export type DeploymentUrls = Readonly<Record<string, string>>;

function validateUrl(value: unknown, deploymentId: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`world-celld: deployment URL for ${deploymentId} must be a non-empty string`);
  }
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`world-celld: deployment URL for ${deploymentId} must be absolute`);
  }
  if (
    (url.protocol !== 'http:' && url.protocol !== 'https:') ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error(`world-celld: deployment URL for ${deploymentId} is invalid`);
  }
  return url.href.replace(/\/$/, '');
}

export function validateDeploymentUrls(value: unknown): DeploymentUrls {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('world-celld: deployment URLs must be an object');
  }
  const urls: Record<string, string> = Object.create(null);
  for (const [id, url] of Object.entries(value)) {
    if (!id || id.trim() !== id) {
      throw new Error('world-celld: deployment URL IDs must be non-empty and unpadded');
    }
    urls[id] = validateUrl(url, id);
  }
  return urls;
}

export function configuredDeploymentUrl(urls: DeploymentUrls, deploymentId: string): string {
  if (!Object.hasOwn(urls, deploymentId)) {
    throw new Error(`world-celld: no callback URL configured for deployment ${deploymentId}`);
  }
  return urls[deploymentId];
}

export function parseDeploymentUrls(value: string | undefined): DeploymentUrls | undefined {
  if (value === undefined || value === '') return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error('world-celld: WORKFLOW_DEPLOYMENT_URLS must be a JSON object');
  }
  return validateDeploymentUrls(parsed);
}
