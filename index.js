require("dotenv").config();
const { Reshuffle, HttpConnector } = require("reshuffle");
const { TwitterV2Connector } = require("../reshuffle-twitter-connector/");
const { GoogleSheetsConnector } = require("reshuffle-google-connectors");
const { PgsqlConnector } = require("reshuffle-pgsql-connector");
const sql = require("sql");

const app = new Reshuffle();

//HTTP Config
const httpConnector = new HttpConnector(app);

//Twitter Config
const twitterConnector = new TwitterV2Connector(app, {
  bearer_token: process.env.BEARER_TOKEN,
});

// PgSql Config
const pg = new PgsqlConnector(app, {
  url: process.env.POSTGRES_URL,
  ssl: { rejectUnauthorized: false },
});

(async () => {
  // ---------- Adding Rules ----------
  // Uncomment to add the rule
  // await twitterConnector.post("tweets/search/stream/rules", {
  //   add: [
  //     { value: "(SNKRS OR snkrs) lang:en -is:retweet -has:images -has:links" },
  //     { value: "(nike OR Nike) lang:en -is:retweet -has:images -has:links" },
  //   ],
  // });

  // ---------- Deleting Rules ----------
  // To delete rules uncomment and add the rule IDs you want to delete
  // await twitterConnector.post("tweets/search/stream/rules", {
  //   delete: {
  //     ids: ["1369860290343436290", "1369860290343436289"],
  //   },
  // });

  // ---------- Get Rules ----------
  // Uncomment to check what rules are set
  // const rules = await twitterConnector.get("tweets/search/stream/rules");
  // console.log(rules);

  // ---------- Adding Rules ----------
  let tweetsToInsert = [];
  let Tweet = sql.define({
    name: "tweets",
    columns: ["id", "text", "author_id", "post_date"],
  });

  const bulkInsert = async (queryString) => {
    await pg.query(queryString);
    tweetsToInsert = [];
  };

  // ---------- Listen to stream ----------
  twitterConnector.on(
    {
      endpoint: "tweets/search/stream",
      parameters: {
        "tweet.fields": "created_at",
        expansions: "author_id",
        "user.fields": "created_at",
      },
    },
    async (event) => {
      if (tweetsToInsert.length === 1000) {
        let query = Tweet.insert(tweetsToInsert)
          .onConflict({ columns: ["id"] })
          .toQuery();

        await bulkInsert(query);
      }
      const { data } = event;

      tweetsToInsert.push({
        id: Number(data.id),
        text: data.text,
        author_id: Number(data.author_id),
        post_date: data.created_at,
      });
    }
  );

  // To check rows in the database uncomment
  // const { rows } = await pg.query(`SELECT * FROM tweets `);
  // console.log(JSON.parse(rows));

  httpConnector.on(
    {
      method: "GET",
      path: "/pg-tweets",
    },
    async (event, app) => {
      const { rows } = await pg.query(`SELECT * FROM tweets `);
      event.res.json(rows);
    }
  );
})();
app.start();
