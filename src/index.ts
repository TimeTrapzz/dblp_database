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
	async fetch(request, env, ctx): Promise<Response> {
		const url = new URL(request.url);
		const query = url.searchParams.get('query') || '';

		if (!query) {
			return new Response(JSON.stringify({
				error: '请在请求参数中提供 query',
			}), { status: 400 });
		}

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

		// 将查询转换为模糊搜索格式
		// const fuzzyQuery = query.split(' ').map(word => word + ':*').join(' & ');

		// const result = await sql`
		// 	SELECT url, title, ts_rank(to_tsvector('english', title), to_tsquery('english', ${fuzzyQuery})) AS rank
		// 	FROM dblp_entries
		// 	WHERE to_tsvector('english', title) @@ to_tsquery('english', ${fuzzyQuery})
		// 	ORDER BY rank DESC
		// 	LIMIT 50;
		// `;

		const searchQuery = query.split(' ').map(word => `${word}:*`).join(' & ');

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
			urls: result.map(item => ({
				url: item.url,
				title: item.title
			}))
		};

		const resp = new Response(JSON.stringify(response), {
			headers: { "Content-Type": "application/json" },
		});

		return resp;
	},
} satisfies ExportedHandler<Env>;
