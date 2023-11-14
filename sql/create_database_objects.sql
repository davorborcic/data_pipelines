CREATE DATABASE pipeline_example;
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(); // verify you are in the right place

CREATE TABLE nba_elo (
gameorder NUMBER,
game_id STRING,
lg_id STRING,
_iscopy NUMBER,
year_id NUMBER,
date_game DATE,
seasongame NUMBER,
is_playoffs NUMBER,
team_id STRING,
fran_id STRING,
pts NUMBER,
elo_i NUMBER,
elo_n NUMBER,
win_equiv NUMBER,
opp_id STRING,
opp_fran STRING,
opp_pts NUMBER,
opp_elo_i NUMBER,
opp_elo_n NUMBER,
game_location STRING,
game_result STRING,
forecast NUMBER,
notes STRING
);