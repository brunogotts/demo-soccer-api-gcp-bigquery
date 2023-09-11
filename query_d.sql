WITH match_goal_scorers AS (
SELECT
  match_id,
  match_hometeam_name,
  match_awayteam_name,
  home_scorer,
  away_scorer
FROM football-analysis-demo.football_api.events t
  LEFT JOIN UNNEST(t.goalscorer) AS unnested
WHERE SAFE_CAST(match_round AS FLOAT64) IS NOT NULL -- Removing all string values, such as: "Round of 16", "Finals",... 
AND CAST(match_round  as INT64) <= 14
),
stacked_goal_scorers AS (
SELECT
  match_hometeam_name AS team_name,
  home_scorer AS scorer
FROM match_goal_scorers
UNION ALL
SELECT
  match_awayteam_name AS team_name,
  away_scorer AS scorer
FROM match_goal_scorers
)
SELECT
  team_name,
  scorer,
  COUNT(*) AS total_scores
FROM stacked_goal_scorers
WHERE scorer != ""
GROUP BY 1,2
ORDER BY 3 DESC, 2 ASC
LIMIT 3