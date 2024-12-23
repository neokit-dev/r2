import { access, defaultPluginOptions, type Plugin, type PluginOptions, inject } from '@neokit-dev/core';
import { defaultNamespace as bns, id as bid, BucketsPlugin } from '@neokit-dev/buckets';
import type { R2Bucket } from '@cloudflare/workers-types';

export const id = 'dev.neokit.r2';
export const defaultNamespace = `${bns}-r2`;
export const apiVersion = 3;
export const version = 2;
export const requires = {
	[bid]: [2, 2]
};

export class R2Plugin extends BucketsPlugin {
	constructor(options: R2PluginOptions) {
		super({
      metadataFn: async (path) => {
        const result = await options.bucket.head(path);
        if (result === null) throw new Error(`File not found: ${path}`);
        return result;
      },
      downloadFn: async (path) => {
        const result = await options.bucket.get(path);
        if (result === null) throw new Error(`File not found: ${path}`);
        return await result.arrayBuffer();
      },
      uploadFn: async (path, data) => {
        return await options.bucket.put(path, data);
      },
      removeFn: async (path) => {
        return await options.bucket.delete(path);
      },
      ...options,
    });
	}
}

export interface R2PluginOptions extends PluginOptions {
	bucket: R2Bucket;
  bucketsNamespace?: string;
}

export function plugin(options: R2PluginOptions): Plugin {
	const p = {
		id,
		version,
		apiVersion,
    requires,
		plugin: new R2Plugin(options),
    onLoaded: () => inject(bid, options.bucketsNamespace ?? bns, p.plugin),
		...defaultPluginOptions(options, { namespace: defaultNamespace })
	};
  return p;
}

export function metadata(path: string) {
  return namespace(bns).metadata(path);
}

export function download(path: string) {
  return namespace(bns).download(path);
}

export function upload(path: string, data: ArrayBuffer) {
  return namespace(bns).upload(path, data);
}

export function remove(path: string) {
  return namespace(bns).remove(path);
}

export function namespace(namespace: string): R2Plugin {
  return access(bid)[namespace].plugin as R2Plugin;
}