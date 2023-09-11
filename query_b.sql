WITH extracting_awayteam_goals AS (
SELECT
  match_id,
  match_awayteam_name,
  CAST(SPLIT(IFNULL(score,"0 - 0"), ' - ')[OFFSET(1)] as INT64) awayteam_goals_scored,
FROM football-analysis-demo.football_api.events t
  LEFT JOIN UNNEST(t.goalscorer) AS unnested
),
final_score_away_team_match AS (
SELECT
  match_id,
  match_awayteam_name,
  MAX(awayteam_goals_scored) AS awayteam_goals_scored
FROM extracting_awayteam_goals
GROUP BY 1,2
)
SELECT
  match_awayteam_name,
  SUM(awayteam_goals_scored) AS total_goals_away
FROM final_score_away_team_match
GROUP BY 1
ORDER BY 2 DESC, 1 ASC