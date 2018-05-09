#include <iostream>
#include <pqxx/pqxx>
#include <cstdlib>
#include <fstream>
//#include <algorithm>
#include "exerciser.h"

using namespace std;
using namespace pqxx;

//Functions for building and populating database

void clearTables(connection * dbConnection){
  if (dbConnection->is_open()){
    
    // SQL statements for dropping existing tables 
    string player_table_q = "DROP TABLE IF EXISTS PLAYER;";
    string team_table_q = "DROP TABLE IF EXISTS TEAM;";
    string state_table_q = "DROP TABLE IF EXISTS STATE;";
    string color_table_q = "DROP TABLE IF EXISTS COLOR;";

    // Transactional object for dropping tables
    work table_drop(*dbConnection);

    try{
      //cout << " **** DROPPING OLD ACC_BBALL TABLES **** " << endl;
      
      table_drop.exec(player_table_q);
      //cout << "Player table has been dropped" << endl;
    
      table_drop.exec(team_table_q);
      //cout << "Team table has been dropped" << endl;
    
      table_drop.exec(state_table_q);
      //cout << "State table has been dropped" << endl;
    
      table_drop.exec(color_table_q);
      table_drop.commit();
      //cout << "Color table has been dropped" << endl;

    } catch(const pqxx_exception& pqe){
      cerr << pqe.base().what() << endl;
      table_drop.abort();
    } catch (const std::exception& e){
      cerr << e.what() << endl;
      table_drop.abort();
    }
  } else{
    cerr << "Error, database connection not open, cannot clear tables" << endl;
  }
}


void makeTables(connection * dbConnection){

  string make_player_table;
  string player;
  string make_team_table;
  string make_state_table;
  string make_color_table;
  
  make_team_table = "CREATE TABLE TEAM("		
    "TEAM_ID  INT PRIMARY KEY                  NOT NULL," 
    "NAME     TEXT                             NOT NULL," 
    "STATE_ID INT REFERENCES STATE(STATE_ID)   NOT NULL," 
    "COLOR_ID INT REFERENCES COLOR(COLOR_ID)   NOT NULL," 
    "WINS     INT                              NOT NULL,"	
    "LOSSES   INT                              NOT NULL);";
  
  make_player_table = "CREATE TABLE PLAYER("	     
    "PLAYER_ID   INT PRIMARY KEY               NOT NULL," 
    "TEAM_ID     INT REFERENCES TEAM(TEAM_ID)  NOT NULL," 
    "UNIFORM_NUM INT                           NOT NULL," 
    "FIRST_NAME  TEXT                          NOT NULL," 
    "LAST_NAME   TEXT                          NOT NULL," 
    "MPG         INT                           NOT NULL,"  // MAYBE HAVE DEFAULT VALUE OF 0
    "PPG         INT                           NOT NULL," 
    "RPG         INT                           NOT NULL," 
    "APG         INT                           NOT NULL," 
    "SPG         DOUBLE PRECISION              NOT NULL," 
    "BPG         DOUBLE PRECISION              NOT NULL);";

  make_state_table = "CREATE TABLE STATE("
    "STATE_ID INT PRIMARY KEY                  NOT NULL,"	
    "NAME                 TEXT                 NOT NULL);";
    
  make_color_table = "CREATE TABLE COLOR("	 
    "COLOR_ID INT PRIMARY KEY                  NOT NULL," 
    "NAME                 TEXT                 NOT NULL);";

  work makeTablesQuery(*dbConnection);

  try{

    //cout << " **** CREATING ACC_BBALL TABLES **** " << endl;
    
    makeTablesQuery.exec(make_state_table);
    //cout << "Made state table" << endl;
    
    makeTablesQuery.exec(make_color_table);
    //cout << "Made color table" << endl;
      
    makeTablesQuery.exec(make_team_table);
    //cout << "Made team table" << endl;
 
    makeTablesQuery.exec(make_player_table);
    //cout << "Made player table" << endl;
 
    makeTablesQuery.commit();
    
  } catch(const pqxx_exception& pqe){
    cerr << pqe.base().what() << endl;
    makeTablesQuery.abort();
  } catch (const std::exception& e){
    cerr << e.what() << endl;
    makeTablesQuery.abort();
  }
}


void populatePlayerTable(connection * dbConnection){
  
  ifstream player_file_stream("player.txt", ifstream::in);
  string current_line, player_id, team_id, uniform_num, first_name, last_name, mpg, ppg,
    rpg, apg, spg, bpg;

  //cout << " **** POPULATING PLAYER TABLE **** " << endl;
  while (player_file_stream.good()){
    getline(player_file_stream, current_line);
    
    size_t delimit_0 = current_line.find_first_of(' ' , 0);
    if (delimit_0 == string::npos){
      //insure the loop terminates even if EOF is not on last valid data line
      break;
    }

    player_id = current_line.substr(0, delimit_0);
    size_t delimit_1 = current_line.find_first_of(' ', delimit_0 + 1);
    
    team_id = current_line.substr(delimit_0 + 1, (delimit_1 - delimit_0) - 1);
    size_t delimit_2 = current_line.find_first_of(' ', delimit_1 + 1);

    uniform_num = current_line.substr(delimit_1 + 1, (delimit_2 - delimit_1) - 1);
    size_t delimit_3 = current_line.find_first_of(' ', delimit_2 + 1);

    first_name = current_line.substr(delimit_2 + 1, (delimit_3 - delimit_2) - 1);
    size_t delimit_4 = current_line.find_first_of(' ', delimit_3 + 1);

    last_name = current_line.substr(delimit_3 + 1, (delimit_4 - delimit_3) - 1);
    size_t delimit_5 = current_line.find_first_of(' ', delimit_4 + 1);

    mpg = current_line.substr(delimit_4 + 1, (delimit_5 - delimit_4) - 1);
    size_t delimit_6 = current_line.find_first_of(' ', delimit_5 + 1);

    ppg = current_line.substr(delimit_5 + 1, (delimit_6 - delimit_5) - 1);
    size_t delimit_7 = current_line.find_first_of(' ', delimit_6 + 1);

    rpg = current_line.substr(delimit_6 + 1, (delimit_7 - delimit_6) - 1);
    size_t delimit_8 = current_line.find_first_of(' ', delimit_7 + 1);

    apg = current_line.substr(delimit_7 + 1, (delimit_8 - delimit_7) - 1);
    size_t delimit_9 = current_line.find_first_of(' ', delimit_8 + 1);

    spg = current_line.substr(delimit_8 + 1, (delimit_9 - delimit_8) - 1);
    bpg = current_line.substr(delimit_9 + 1, string::npos);

    /*
      cout << "Line read = '" << current_line << "' || player id = '" << player_id <<
      "' || team_id = '" << team_id << "'\n"  <<"|| uniform_num = '" << uniform_num <<
      "' || first_name = '" << first_name << "' || last_name = '" << last_name <<
      "' || mpg = '" << mpg << "|| ppg = '" << ppg << " || rpg = '" << rpg << "'\n" <<
      "|| apg = '" << apg << " || spg = '" << spg << " || bpg = '" << bpg << "' || ";
    */
    
    work addPlayerQuery(*dbConnection);

    try{
      string player_insert = "INSERT INTO PLAYER (PLAYER_ID,TEAM_ID,UNIFORM_NUM,FIRST_NAME,"
	"LAST_NAME,MPG,PPG,RPG,APG,SPG,BPG) VALUES (" + player_id + ", " + team_id + ", " +
	uniform_num + ", " + addPlayerQuery.quote(first_name) + ", " +
	addPlayerQuery.quote(last_name) + ", " + mpg + ", " + ppg + ", " + rpg + ", " + apg +
	", " + spg + ", " + bpg + ");";

      addPlayerQuery.exec(player_insert);

      addPlayerQuery.commit();

    } catch(const pqxx_exception& pqe){
      cerr << pqe.base().what() << endl;
      addPlayerQuery.abort();
    } catch (const std::exception& e){
      cerr << e.what() << endl;
      addPlayerQuery.abort();
    }
  }
  player_file_stream.close();
}


void populateTeamTable(connection * dbConnection){
  
  ifstream team_file_stream("team.txt", ifstream::in);
  string current_line, team_id, team_name, state_id, color_id, wins, losses;

  //cout << " **** POPULATING TEAM TABLE **** " << endl;
  
  while (team_file_stream.good()){
    getline(team_file_stream, current_line);
    
    size_t delimit_0 = current_line.find_first_of(' ' , 0);
    if (delimit_0 == string::npos){
      //insure the loop terminates even if EOF is not on last valid data line
      break;
    }
      
    team_id = current_line.substr(0, delimit_0);
    size_t delimit_1 = current_line.find_first_of(' ', delimit_0 + 1);

    team_name = current_line.substr(delimit_0 + 1, (delimit_1 - delimit_0) - 1);
    size_t delimit_2 = current_line.find_first_of(' ', delimit_1 + 1);

    state_id = current_line.substr(delimit_1 + 1, (delimit_2 - delimit_1) - 1);
    size_t delimit_3 = current_line.find_first_of(' ', delimit_2 + 1);

    color_id = current_line.substr(delimit_2 + 1, (delimit_3 - delimit_2) - 1);
    size_t delimit_4 = current_line.find_first_of(' ', delimit_3 + 1);

    wins = current_line.substr(delimit_3 + 1, (delimit_4 - delimit_3) - 1);
    losses = current_line.substr(delimit_4 + 1, string::npos);

    /*
      cout << "Line read = '" << current_line << "' || team id = '" << team_id <<
      "' || team_name = '" << team_name << "' || state_id = '" << state_id << "'\n" <<
      "|| color_id = '" << color_id << "' || wins = '" << wins <<
      "' || losses = '" << losses << "' || ";
    */
    
    work addTeamQuery(*dbConnection);

    try{
      string team_insert = "INSERT INTO TEAM (TEAM_ID,NAME,STATE_ID,COLOR_ID,WINS,LOSSES) "
	"VALUES (" + team_id + ", " + addTeamQuery.quote(team_name) + ", " + state_id + ", "
	+ color_id + ", " + wins + ", " + losses + ");";

      addTeamQuery.exec(team_insert);
      addTeamQuery.commit();

    } catch(const pqxx_exception& pqe){
      cerr << pqe.base().what() << endl;
      addTeamQuery.abort();
    } catch (const std::exception& e){
      cerr << e.what() << endl;
      addTeamQuery.abort();
    }
  }
  team_file_stream.close();
}

void populateStateTable(connection * dbConnection){
  
  ifstream state_file_stream("state.txt", ifstream::in);
  string current_line, state_id, state_name;

  //cout << " **** POPULATING STATE TABLE **** " << endl;

  while (state_file_stream.good()){
    getline(state_file_stream, current_line);
    
    size_t space_delimiter = current_line.find_first_of(' ' , 0);
    if (space_delimiter == string::npos){
      //insure the loop terminates even if EOF is not on last valid data line
      break;
    }
      
    state_id = current_line.substr(0, space_delimiter);
    state_name = current_line.substr(space_delimiter + 1, string::npos);

    /*
      cout << "Line read = '" << current_line << "' || state id = '" << state_id <<
      "' || state_name = '" << state_name << "' || ";
    */
    
    work addStateQuery(*dbConnection);

    try{
      string state_insert = "INSERT INTO STATE (STATE_ID,NAME) "
	"VALUES (" + state_id + ", " + addStateQuery.quote(state_name) + ");";

      addStateQuery.exec(state_insert);
      addStateQuery.commit();
      
    } catch(const pqxx_exception& pqe){
      cerr << pqe.base().what() << endl;
      addStateQuery.abort();
    } catch (const std::exception& e){
      cerr << e.what() << endl;
      addStateQuery.abort();
    }
  }
  state_file_stream.close();
}


void populateColorTable(connection * dbConnection){

  ifstream color_file_stream("color.txt", ifstream::in);
  string current_line, color_id, color_name;

  //cout << " **** POPULATING COLOR TABLE **** " << endl;
  
  while (color_file_stream.good()){
    getline(color_file_stream, current_line);
    
    size_t space_delimiter = current_line.find_first_of(' ' , 0);
    if (space_delimiter == string::npos){
      //insure the loop terminates even if EOF is not on last valid data line
      break;
    }
    
    color_id = current_line.substr(0, space_delimiter);
    color_name = current_line.substr(space_delimiter + 1, string::npos);

    /*
      cout << "Line read = '" << current_line << "' || color id = '" << color_id <<
      "' || color_name = '" << color_name << "' || ";
    */
    
    work addColorQuery(*dbConnection);
    try{
      
      string color_insert = "INSERT INTO COLOR (COLOR_ID,NAME) "
	"VALUES (" + color_id + ", " + addColorQuery.quote(color_name) + ");";
      
      addColorQuery.exec(color_insert);
      addColorQuery.commit();
      

    } catch(const pqxx_exception& pqe){
      cerr << pqe.base().what() << endl;
      addColorQuery.abort();
    } catch (const std::exception& e){
      cerr << e.what() << endl;
      addColorQuery.abort();
    }
  }
  color_file_stream.close();
}



int main (int argc, char *argv[]) {

  //Allocate & initialize a Postgres connection object
  connection *C;
  
  try{
    //Establish a connection to the database
    //Parameters: database name, user name, user password
    C = new connection("dbname=acc_bball user=postgres password=passw0rd");
   
    if (C->is_open()) {
      cout << "Opened database successfully: " << C->dbname() << endl;
    } else {
      cout << "Can't open database" << endl;
      return 1;
    }

    //Clear existing tables
    clearTables(C);

    //Reconstruct tables
    makeTables(C);

    //Populate tables with data
    populateColorTable(C);

    populateStateTable(C);

    populateTeamTable(C);

    populatePlayerTable(C);
    
  } catch (const std::exception &e){
    cerr << e.what() << std::endl;
    cerr << "Failed to reconstruct and populate database " << C->dbname() << endl;
    return EXIT_FAILURE;
  }

  //Test the database
  exercise(C);

  //Close database connection
  C->disconnect();

  return EXIT_SUCCESS;
}


