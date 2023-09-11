WITH extracting_match_score AS (
SELECT
  match_id,
  match_hometeam_name,
  match_awayteam_name,
  IFNULL(score,"0 - 0") AS score,
FROM football-analysis-demo.football_api.events t
  LEFT JOIN UNNEST(t.goalscorer) AS unnested
WHERE match_status != "Cancelled"
),
home_away_goals AS (
SELECT
  match_id,
  match_hometeam_name,
  match_awayteam_name,
  CAST(SPLIT(score, ' - ')[OFFSET(0)] as INT64) hometeam_goals_scored,
  CAST(SPLIT(score, ' - ')[OFFSET(1)] as INT64) awayteam_goals_scored,
FROM
  extracting_match_score
),
final_score_match AS (
SELECT
  match_id,
  match_hometeam_name,
  match_awayteam_name,
  MAX(hometeam_goals_scored) AS hometeam_goals_scored,
  MAX(awayteam_goals_scored) AS awayteam_goals_scored
FROM home_away_goals
GROUP BY 1,2,3
),
home_away_match_statistics AS (
SELECT
  match_hometeam_name,
  hometeam_goals_scored,
  IF(hometeam_goals_scored > awayteam_goals_scored, 1,0) AS hometeam_won,
  IF(hometeam_goals_scored = awayteam_goals_scored, 1,0) AS hometeam_tie,
  IF(hometeam_goals_scored < awayteam_goals_scored, 1,0) AS hometeam_lost,
  awayteam_goals_scored AS hometeam_goals_conceded,
  match_awayteam_name,
  awayteam_goals_scored,
  IF(hometeam_goals_scored < awayteam_goals_scored, 1,0) AS awayteam_won,
  IF(hometeam_goals_scored = awayteam_goals_scored, 1,0) AS awayteam_tie,
  IF(hometeam_goals_scored > awayteam_goals_scored, 1,0) AS awayteam_lost,
  hometeam_goals_scored AS awayteam_goals_conceded,
FROM 
  final_score_match
),
stacking_teams_statistics AS (
SELECT 
  match_hometeam_name AS team_name,
  hometeam_won AS won,
  hometeam_tie AS tie,
  hometeam_lost AS lost,
  hometeam_goals_scored AS goals_scored,
  hometeam_goals_conceded AS goals_conceded
FROM
  home_away_match_statistics
UNION ALL
SELECT 
  match_awayteam_name AS team_name,
  awayteam_won AS won,
  awayteam_tie AS tie,
  awayteam_lost AS lost,
  awayteam_goals_scored AS goals_scored,
  awayteam_goals_conceded AS goals_conceded
FROM
  home_away_match_statistics
),
total_teams_statistics AS (
SELECT
  team_name,
  COUNT(team_name) AS matches_played,
  SUM(won) AS won,
  SUM(tie) AS tie,
  SUM(lost) AS lost,
  SUM(goals_scored) AS goals_scored,
  SUM(goals_conceded) AS goals_conceded
FROM 
  stacking_teams_statistics
GROUP BY 1
),
calculating_metrics AS (
SELECT
  *,
  (goals_scored - goals_conceded) AS goals_difference,
  (won * 3 + tie * 1) AS points
FROM
  total_teams_statistics
)
SELECT
  *,
  DENSE_RANK() OVER ( ORDER BY points DESC, goals_difference DESC, goals_scored DESC, goals_conceded ASC, won DESC ) AS rank
FROM
  calculating_metrics
ORDER BY rank