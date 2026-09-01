type CacheEntry<T> = {
	value: T;
	expiresAt: number;
};

const store = new Map<string, CacheEntry<unknown>>();
const inflight = new Map<string, Promise<unknown>>();

export async function getOrSetCache<T>(
	key: string,
	ttlMs: number,
	loader: () => Promise<T>
): Promise<T> {
	const now = Date.now();
	const cached = store.get(key);

	if (cached && cached.expiresAt > now) {
		return cached.value as T;
	}

	const pending = inflight.get(key);
	if (pending) {
		return pending as Promise<T>;
	}

	const promise = loader()
		.then((value) => {
			store.set(key, { value, expiresAt: Date.now() + ttlMs });
			return value;
		})
		.finally(() => {
			inflight.delete(key);
		});

	inflight.set(key, promise);
	return promise;
}

export function clearCache(key: string): void {
	store.delete(key);
	inflight.delete(key);
}
