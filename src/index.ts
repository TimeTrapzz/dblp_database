/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Bind resources to your worker in `wrangler.toml`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */
import postgres from "postgres";

export default {
	async fetch(request: any, env: any, ctx: any): Promise<Response> {
		const sql = postgres({
			username: env.DB_USERNAME,
			password: env.DB_PASSWORD,
			host: env.DB_HOST,
			port: env.DB_PORT,
			database: env.DB_NAME,
			ssl: {
				rejectUnauthorized: true
			}
		});
		if (request.method == 'GET') {
			return handleGet(request, sql);
		}
		else if (request.method == 'POST') {
			return handlePost(request, sql);
		}
		else {
			return new Response(JSON.stringify({
				error: '不支持的请求方法',
			}), { status: 405 });
		}
	},
} satisfies ExportedHandler<Env>;

async function handleGet(request: any, sql: any): Promise<Response> {
	const url = new URL(request.url);
	const query = url.searchParams.get('query') || '';
	if (query.length == 0) {
		return new Response(JSON.stringify({
			error: '请输入搜索关键词',
		}), {
			status: 400,
			headers: { "Content-Type": "application/json" }
		});
	}
	const searchQuery = query.split(' ').map((word: any) => `${word}:*`).join(' & ');
	const result = await sql`
    		SELECT url, title,
           	ts_rank_cd(to_tsvector('english', title), to_tsquery('english', ${searchQuery})) AS rank
    		FROM dblp_entries
			WHERE to_tsvector('english', title) @@ to_tsquery('english', ${searchQuery})
			ORDER BY rank DESC
			LIMIT 50;
		`;

	const response = {
		query: query,
		urls: result.map((item: any) => ({
			url: item.url,
			title: item.title
		}))
	};

	const resp = new Response(JSON.stringify(response), {
		headers: { "Content-Type": "application/json" },
	});

	return resp;

}

async function handlePost(request: any, sql: any): Promise<Response> {
	const body = await request.json();
	const queries = body.queries;

	if (!Array.isArray(queries) || queries.length === 0) {
		return new Response(JSON.stringify({ error: '无效的查询列表' }), {
			status: 400,
			headers: { "Content-Type": "application/json" }
		});
	}

	const searchQueries = queries.map(query => {
		return `${query}:*`;
	});
	
	// Execute a single SQL query for all search queries
	const result = await sql`
        SELECT 
            q.query_index,
            e.url,
            e.title,
            ts_rank_cd(to_tsvector('english', e.title), to_tsquery('english', q.query)) AS rank
        FROM 
            unnest(${searchQueries}::text[]) WITH ORDINALITY AS q(query, query_index)
        CROSS JOIN LATERAL (
            SELECT url, title
            FROM dblp_entries
            WHERE to_tsvector('english', title) @@ to_tsquery('english', q.query)
            ORDER BY ts_rank_cd(to_tsvector('english', title), to_tsquery('english', q.query)) DESC
            LIMIT 50
        ) e
        ORDER BY q.query_index, rank DESC
    `;

	const results = queries.map((query, index) => ({
		query: query,
		urls: result
			.filter((item: any) => item.query_index == index + 1)
			.map((item: any) => ({
				url: item.url,
				title: item.title
			}))
	}));

	const resp = new Response(JSON.stringify(results), {
		headers: { "Content-Type": "application/json" }
	});

	return resp;
}
