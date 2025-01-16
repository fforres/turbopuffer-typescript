import { version } from "../package.json";
import { gunzip, gzip } from "node:zlib";
import { promisify } from "node:util";

const gunzipAsync = promisify(gunzip);
const gzipAsync = promisify(gzip);

export interface RequestParams {
  method: string;
  path: string;
  query?: Record<string, string | undefined>;
  body?: unknown;
  compress?: boolean;
  retryable?: boolean;
}

export interface RequestTiming {
  response_time: number;
  body_read_time: number;
  decompress_time: number;
  compress_time: number;
  deserialize_time: number;
}

export type RequestResponse<T> = Promise<{
  body?: T;
  headers: Record<string, string>;
  request_timing: RequestTiming;
}>;

export interface HTTPClient {
  doRequest<T>(_: RequestParams): RequestResponse<T>;
}

/**
 * Returns a class for making fetch requests against the API.
 *
 * @param baseUrl The base URL of the API endpoint.
 * @param apiKey The API key to use for authentication.
 */
export const createHTTPClient = (
  baseUrl: string,
  apiKey: string,
  connectTimeout: number,
  idleTimeout: number, // no longer used with fetch
  warmConnections: number, // will just do HEAD requests
  compression: boolean,
) =>
  new DefaultHTTPClient(
    baseUrl,
    apiKey,
    connectTimeout,
    idleTimeout,
    warmConnections,
    compression,
  );

class DefaultHTTPClient implements HTTPClient {
  private connectTimeout: number;
  private baseUrl: string;
  private origin: URL;
  private apiKey: string;
  readonly userAgent = `tpuf-typescript/${version}`;
  private compression: boolean;

  constructor(
    baseUrl: string,
    apiKey: string,
    connectTimeout: number,
    idleTimeout: number,
    warmConnections: number,
    compression: boolean,
  ) {
    this.baseUrl = baseUrl;
    this.origin = new URL(baseUrl);
    this.origin.pathname = "";
    this.apiKey = apiKey;
    this.compression = compression;
    this.connectTimeout = connectTimeout;

    // "Warm up" connections by doing HEAD requests.
    // In environments like Cloudflare Workers, this doesn't necessarily
    // do the same "connection pooling" you'd get in Node.js with an agent,
    // but we replicate the logic for consistency.
    for (let i = 0; i < warmConnections; i++) {
      void fetch(this.baseUrl, {
        method: "HEAD",
        headers: { "User-Agent": this.userAgent },
      }).catch(() => {
        // Non-critical error if warm-up fails; ignore or log.
      });
    }
  }

  async doRequest<T>({
    method,
    path,
    query,
    body,
    compress,
    retryable,
  }: RequestParams): RequestResponse<T> {
    const url = new URL(`${this.baseUrl}${path}`);
    if (query) {
      for (const key in query) {
        const value = query[key];
        if (value) {
          url.searchParams.append(key, value);
        }
      }
    }

    const headers: Record<string, string> = {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      Authorization: `Bearer ${this.apiKey}`,
      // eslint-disable-next-line @typescript-eslint/naming-convention
      "User-Agent": this.userAgent,
    };

    if (this.compression) {
      headers["Accept-Encoding"] = "gzip";
    }

    if (body) {
      headers["Content-Type"] = "application/json";
    }

    let requestCompressionDuration: number | undefined;
    let requestBody: Uint8Array | string | null = null;
    if (body && compress && this.compression) {
      headers["Content-Encoding"] = "gzip";
      const beforeRequestCompression = performance.now();
      requestBody = await gzipAsync(JSON.stringify(body));
      requestCompressionDuration = performance.now() - beforeRequestCompression;
    } else if (body) {
      requestBody = JSON.stringify(body);
    }

    const maxAttempts = retryable ? 3 : 1;
    let response!: Response;
    let error: TurbopufferError | null = null;
    let request_start!: number;
    let response_start!: number;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      error = null;
      request_start = performance.now();
      // Use an AbortController to implement connectTimeout
      const controller = new AbortController();
      const { signal } = controller;
      const timeoutHandle = setTimeout(() => {
        controller.abort();
      }, this.connectTimeout);
      try {
        response = await fetch(url.toString(), {
          method,
          signal, // attach the abort signal
          headers,
          body: requestBody,
        });
      } catch (e: unknown) {
        if (e instanceof Error) {
          // Check if it was an abort error vs. another fetch error
          if (e.name === "AbortError") {
            error = new TurbopufferError(
              `fetch aborted (connectTimeout=${this.connectTimeout}ms)`,
              { cause: e },
            );
          } else {
            error = new TurbopufferError(`fetch failed: ${e.message}`, {
              cause: e,
            });
          }
        } else {
          // not an Error? Rare, but handle gracefully
          throw e;
        }
      } finally {
        clearTimeout(timeoutHandle);
      }

      response_start = performance.now();

      if (!error && response) {
        if (response.status >= 400) {
          let message: string | undefined;
          const body_text = await response.text();

          // Attempt JSON parse for a more descriptive error
          if (response.headers.get("content-type") === "application/json") {
            try {
              const parsedBody = JSON.parse(body_text) as {
                status?: string;
                error?: string;
              };
              if (parsedBody && parsedBody.status === "error") {
                message = parsedBody.error;
              } else {
                message = body_text;
              }
            } catch {
              message = body_text;
            }
          } else {
            message = body_text;
          }

          error = new TurbopufferError(
            message ?? `http error ${response.status}`,
            {
              status: response.status,
            },
          );
        }
      }

      // Retry on 5xx or no status
      if (
        error &&
        statusCodeShouldRetry(error.status) &&
        attempt + 1 !== maxAttempts
      ) {
        await delay(150 * (attempt + 1)); // 150ms, 300ms, 450ms
        continue;
      }
      break;
    }

    if (error) {
      throw error;
    }

    // If HEAD or no response, return early
    if (!response || method === "HEAD") {
      return {
        headers: convertHeadersType(
          response ? Object.fromEntries(response.headers) : {},
        ),
        request_timing: make_request_timing(request_start, response_start),
      };
    }

    // For non-HEAD requests, we read the body:
    const body_read_end: number = performance.now();
    const body_text = await response.text();
    const decompress_end = performance.now();

    const json = JSON.parse(body_text) as {
      status?: string;
      error?: string;
    };
    const deserialize_end = performance.now();

    if (json.status && json.status === "error") {
      throw new TurbopufferError(json.error || (json as string), {
        status: response.status,
      });
    }

    return {
      body: json as T,
      headers: convertHeadersType(Object.fromEntries(response.headers)),
      request_timing: make_request_timing(
        request_start,
        response_start,
        body_read_end,
        decompress_end,
        deserialize_end,
        requestCompressionDuration,
      ),
    };
  }
}

/** An error class for errors returned by the turbopuffer API. */
export class TurbopufferError extends Error {
  status?: number;
  constructor(
    public error: string,
    { status, cause }: { status?: number; cause?: Error },
  ) {
    super(error, { cause });
    this.status = status;
  }
}

/** A helper function to determine if a status code should be retried. */
function statusCodeShouldRetry(statusCode?: number): boolean {
  return !statusCode || statusCode >= 500;
}

/** A helper function to delay for a given number of milliseconds. */
function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function make_request_timing(
  request_start: number,
  response_start: number,
  body_read_end?: number,
  decompress_end?: number,
  deserialize_end?: number,
  requestCompressionDuration?: number,
): RequestTiming {
  return {
    response_time: response_start - request_start,
    body_read_time: body_read_end ? body_read_end - response_start : 0,
    compress_time: requestCompressionDuration ?? 0,
    decompress_time:
      decompress_end && body_read_end ? decompress_end - body_read_end : 0,
    deserialize_time:
      deserialize_end && decompress_end ? deserialize_end - decompress_end : 0,
  };
}

function convertHeadersType(
  headers: Record<string, string>,
): Record<string, string> {
  // For Cloudflare Workers / standard fetch: it's already simplified key/value pairs
  // We just ensure it's typed as a plain record.
  return headers;
}
