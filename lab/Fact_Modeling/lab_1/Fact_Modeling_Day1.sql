Create table Jaswanthv.fct_nba_game_details 
(
	game_id BIGINT,
	team_id BIGINT,
	player_id BIGINT,
	dim_team_abbreviation VARCHAR,
	dim_player_name VARCHAR,
	dim_start_position VARCHAR,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	m_seconds_played INTEGER,
	m_field_goals_made DOUBLE,
	m_field_goals_attempted DOUBLE,
	m_3_pointers_made DOUBLE,
	m_3_pointers_attempted DOUBLE,
	m_free_throws_made DOUBLE,
	m_free_throws_attempted DOUBLE,
	m_offensive_rebounds DOUBLE,
	m_defensive_rebounds DOUBLE,
	m_rebounds DOUBLE,
	m_assists DOUBLE,
	m_steals DOUBLE,
	m_blocks DOUBLE,
	m_turnovers DOUBLE,
	m_personal_fouls DOUBLE,
	m_points DOUBLE,
	m_plus_minus DOUBLE,
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_did_win BOOLEAN
)
WITH
(
 FORMAT = 'PARQUET',
 partitioning = ARRAY['dim_season']
);

INSERT INTO
  Jaswanthv.fct_nba_game_details
With games As (
Select 
  game_id,
  season,
  home_team_wins,
  home_team_id,
  visitor_team_id,
  game_date_est
 from bootcamp.nba_games
)
Select 
	CAST(g.game_id AS BIGINT) AS game_id,
	gd.team_id,
	gd.player_id,
	gd.team_abbreviation AS dim_team_abbreviation,
	gd.player_name AS dim_player_name,
	gd.start_position AS dim_start_position,
	gd.comment LIKE '%DND%' AS dim_did_not_dress,
	gd.comment LIKE '%NWT%' AS dim_not_with_team,
	CASE
    	WHEN CARDINALITY(SPLIT(MIN, ':')) > 1 
			THEN CAST(SPLIT(MIN, ':') [1] AS DOUBLE) * 60 + CAST(SPLIT(MIN, ':') [2] AS DOUBLE) 
  			ELSE CAST(MIN AS INTEGER) 
  	END AS m_seconds_played,
    CAST(fgm AS DOUBLE) AS m_field_goals_made,
    CAST(fga AS DOUBLE) AS m_field_goals_attempted,
    CAST(fg3m AS DOUBLE) AS m_3_pointers_made,
    CAST(fg3a AS DOUBLE) AS m_3_pointers_attempted,
    CAST(ftm AS DOUBLE) AS m_free_throws_made,
    CAST(fta AS DOUBLE) AS m_free_throws_attempted,
    CAST(oreb AS DOUBLE) AS m_offensive_rebounds,
    CAST(dreb AS DOUBLE) AS m_defensive_rebounds,
    CAST(reb AS DOUBLE) AS m_rebounds,
    CAST(ast AS DOUBLE) AS m_assists,
    CAST(stl AS DOUBLE) AS m_steals,
    CAST(blk AS DOUBLE) AS m_blocks,
    CAST(TO AS DOUBLE) AS m_turnovers,
    CAST(pf AS DOUBLE) AS m_personal_fouls,
    CAST(pts AS DOUBLE) AS m_points,
    CAST(plus_minus AS DOUBLE) AS m_plus_minus,
    g.game_date_est AS dim_game_date,
    g.season AS dim_season,	
  	CASE
    	WHEN gd.team_id = g.home_team_id 
			THEN home_team_wins = 1
    		ELSE home_team_wins = 0
   	END AS dim_team_did_win 
from games g JOIN bootcamp.nba_game_details gd on g.game_id = gd.game_id;


SELECT
  YEAR(dim_game_date),
  dim_team_did_win,
  AVG(m_points)
FROM
  Jaswanthv.fct_nba_game_details
WHERE
  dim_player_name = 'LeBron James'
  GROUP BY 
    YEAR(dim_game_date),
    dim_team_did_win
    ORDER BY 1,2
	
	-- Number of players who have played for multiple teams
	
	SELECT
	  game_id,
	  team_id,
	  player_id,
	  COUNT(1)
	FROM
	  Jaswanthv.fct_nba_game_details
	GROUP BY
	  game_id,
	  team_id,
	  player_id
	HAVING
	  COUNT(1) > 1