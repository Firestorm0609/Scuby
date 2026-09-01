import { fetchHomepageData } from '$lib/server/boxes.js';
import type { PageServerLoad } from './$types.js';

export const load: PageServerLoad = async ({ setHeaders, url }) => {
	const test = url.searchParams.has('test');
	setHeaders({
		'cache-control': 'public, max-age=0, s-maxage=30, stale-while-revalidate=300'
	});

	return fetchHomepageData(test);
};
