#include "query_funcs.h"
#include <algorithm>
#include <tuple>

void add_player(connection *C, int team_id, int jersey_num, string first_name, string last_name,
                int mpg, int ppg, int rpg, int apg, double spg, double bpg){
  
  nontransaction addPlayerQuery(*C);

  try{
    string findMax = "SELECT MAX(PLAYER_ID) FROM PLAYER;";
    result maxResult(addPlayerQuery.exec(findMax));

    result::const_iterator c = maxResult.begin();
    int current_max = c[0].as<int>();
  
    //cout << "Found max primary key from db = " << current_max << endl;

    int player_id = current_max + 1;

    /*
      cout << "Player id (made) = '" << player_id <<
      "' || team_id = '" << team_id << "'\n"  <<"|| uniform_num = '" << jersey_num <<
      "' || first_name = '" << first_name << "' || last_name = '" << last_name <<
      "' || mpg = '" << mpg << "|| ppg = '" << ppg << " || rpg = '" << rpg << "'\n" <<
      "|| apg = '" << apg << " || spg = '" << spg << " || bpg = '" << bpg << "' || ";
    */
      
    string player_insert = "INSERT INTO PLAYER (PLAYER_ID,TEAM_ID,UNIFORM_NUM,FIRST_NAME,"
      "LAST_NAME,MPG,PPG,RPG,APG,SPG,BPG) VALUES (" + to_string(player_id) + ", " +
      to_string(team_id) + ", " + to_string(jersey_num) + ", " +
      addPlayerQuery.quote(first_name) + ", " + addPlayerQuery.quote(last_name) + ", " +
      to_string(mpg) + ", " + to_string(ppg) + ", " + to_string(rpg) + ", " + to_string(apg) +
      ", " + to_string(spg) + ", " + to_string(bpg) + ");";

    addPlayerQuery.exec(player_insert);
    addPlayerQuery.commit();
    //cout << "Added new player with id = " << player_id << " to PLAYER table" << endl;

  } catch(const pqxx_exception& pqe){
    cerr << pqe.base().what() << endl;
    cerr << "Performing rollback from add_player()... " << endl;
    addPlayerQuery.abort();
  } catch (const std::exception &e){
    cerr << e.what() << endl;
    cerr << "Performing rollback from add_player()... " << endl;
    addPlayerQuery.abort();
  }
}


void add_team(connection *C, string name, int state_id, int color_id, int wins, int losses){
  
  nontransaction addTeamQuery(*C);

  try{
    string findMax = "SELECT MAX(TEAM_ID) FROM TEAM;";
    result maxResult(addTeamQuery.exec(findMax));

    result::const_iterator c = maxResult.begin();
    int current_max = c[0].as<int>();
  
    //cout << "Found max primary key from db = " << current_max << endl;

    int team_id = current_max + 1;

    string team_insert = "INSERT INTO TEAM (TEAM_ID,NAME,STATE_ID,COLOR_ID,WINS,LOSSES) "
      "VALUES (" + to_string(team_id) + ", " + addTeamQuery.quote(name) + ", " +
      to_string(state_id) + ", " + to_string(color_id) + ", " + to_string(wins) + ", " +
      to_string(losses) + ");";
    
    addTeamQuery.exec(team_insert);
    addTeamQuery.commit();
    //cout << "Added new team with id = " << team_id << " to TEAM table" << endl;

  } catch(const pqxx_exception& pqe){
    cerr << pqe.base().what() << endl;
    cerr << "Performing rollback from add_team()... " << endl;
    addTeamQuery.abort();
  } catch (const std::exception &e){
    cerr << e.what() << endl;
    cerr << "Performing rollback from add_team()... " << endl;
    addTeamQuery.abort();
  }
}


void add_state(connection *C, string name){
  
  nontransaction addStateQuery(*C);
  
  try{
    string findMax = "SELECT MAX(STATE_ID) FROM STATE;";
    result maxResult(addStateQuery.exec(findMax));

    result::const_iterator c = maxResult.begin();
    int current_max = c[0].as<int>();
  
    //cout << "Found max primary key from db = " << current_max << endl;

    int state_id = current_max + 1;

    string state_insert = "INSERT INTO STATE (STATE_ID,NAME) "
      "VALUES (" + to_string(state_id) + ", " + addStateQuery.quote(name) + ");";
  
    addStateQuery.exec(state_insert);
    addStateQuery.commit();
    //cout << "Added new state with id = " << state_id << " to STATE table" << endl;

  } catch(const pqxx_exception& pqe){
    cerr << pqe.base().what() << endl;
    cerr << "Performing rollback from add_state()... " << endl;
    addStateQuery.abort();
  } catch (const std::exception &e){
    cerr << e.what() << endl;
    cerr << "Performing rollback from add_state()... " << endl;
    addStateQuery.abort();
  }
}


void add_color(connection *C, string name){
 
  nontransaction addColorQuery(*C);
  
  try{
    string findMax = "SELECT MAX(COLOR_ID) FROM COLOR;";
    result maxResult(addColorQuery.exec(findMax));

    result::const_iterator c = maxResult.begin();
    int current_max = c[0].as<int>();

    int color_id = current_max + 1;
    //cout << "Found max primary key from db = " << current_max << endl;

    string color_insert = "INSERT INTO COLOR (COLOR_ID,NAME) "
      "VALUES (" + to_string(color_id) + ", " + addColorQuery.quote(name) + ");";
  
    addColorQuery.exec(color_insert);
    addColorQuery.commit();
    //cout << "Added new color with id = " << color_id << " to COLOR table" << endl;

  } catch(const pqxx_exception& pqe){
    cerr << pqe.base().what() << endl;
    cerr << "Performing rollback from add_player()... " << endl;
    addColorQuery.abort();
  } catch (const std::exception &e){
    cerr << e.what() << endl;
    cerr << "Performing rollback from add_color()... " << endl;
    addColorQuery.abort();
  }
}


void query1(connection *C,
	    int use_mpg, int min_mpg, int max_mpg,
            int use_ppg, int min_ppg, int max_ppg,
            int use_rpg, int min_rpg, int max_rpg,
            int use_apg, int min_apg, int max_apg,
            int use_spg, double min_spg, double max_spg,
            int use_bpg, double min_bpg, double max_bpg
            ){

  work q1(*C);

  int indicator = 0;
  
  string sql = "SELECT * FROM PLAYER ";
  if (use_mpg || use_ppg || use_rpg || use_apg || use_spg || use_bpg){
    sql += "WHERE "; 
  }
  
  if (use_mpg){
    indicator++;
    sql += "(MPG >= " + to_string(min_mpg) + " AND MPG <= " + to_string(max_mpg) + ") ";
  }
  if (use_ppg){
    if (indicator){
      sql += "AND (PPG >= " + to_string(min_ppg) + " AND PPG <= " + to_string(max_ppg) + ") ";
    } else{
      sql += "(PPG >= " + to_string(min_ppg) + " AND PPG <= " + to_string(max_ppg) + ") ";
    }
    indicator++;
  }
  if (use_rpg){
    if (indicator){
      sql += "AND (RPG >= " + to_string(min_rpg) + " AND RPG <= " + to_string(max_rpg) + ") ";
    } else{
      sql += "(RPG >= " + to_string(min_rpg) + " AND RPG <= " + to_string(max_rpg) + ") ";
    }
    indicator++;
  }
  if (use_apg){
    if (indicator){
      sql += "AND (APG >= " + to_string(min_apg) + " AND APG <= " + to_string(max_apg) + ") ";
    } else{
      sql += "(APG >= " + to_string(min_apg) + " AND APG <= " + to_string(max_apg) + ") ";
    }
    indicator++;
  }
  if (use_spg){
    if (indicator){
      sql += "AND (SPG >= " + to_string(min_spg) + " AND SPG <= " + to_string(max_spg) + ") ";
    } else{
      sql += "(SPG >= " + to_string(min_spg) + " AND SPG <= " + to_string(max_spg) + ") ";
    }
    indicator++;
  }
  if (use_bpg){
    if (indicator){
      sql += "AND (BPG >= " + to_string(min_bpg) + " AND BPG <= " + to_string(max_bpg) + ")";
    } else{
      sql += "(BPG >= " + to_string(min_bpg) + " AND BPG <= " + to_string(max_bpg) + ")";
    }
  }

  sql +=  ";";

  //cout << "Query1: " << sql << endl;
  
  try{

    result r(q1.exec(sql));
    //list the query results
    cout << "PLAYER_ID TEAM_ID UNIFORM_NUM FIRST_NAME LAST_NAME MPG PPG RPG APG SPG BPG" << endl;
    
    for (result::const_iterator it = r.begin(); it != r.end(); ++it){
      cout << it[0].as<int>() << " " << it[1].as<int>() << " " << it[2].as<int>() << " " <<
	it[3].as<string>() << " " << it[4].as<string>() << " " << it[5].as<int>() << " " <<
	it[6].as<int>() << " " << it[7].as<int>() << " " << it[8].as<int>() << " " <<
	it[9].as<double>() << " " << it[10].as<double>() << endl;
    }


  } catch(const pqxx_exception& pqe){
    cerr << pqe.base().what() << endl;
    cerr << "Performing rollback from query1()... " << endl;
    q1.abort();
  } catch (const std::exception &e){
    cerr << e.what() << endl;
    cerr << "Performing rollback from query1()... " << endl;
    q1.abort();
  }
}


void query2(connection *C, string team_color){
  work q2(*C);
  
  try{
    string sql = "SELECT TEAM.NAME FROM TEAM, COLOR "
      "WHERE TEAM.COLOR_ID=COLOR.COLOR_ID AND COLOR.NAME=" + q2.quote(team_color) + ";";

    result r(q2.exec(sql));
    //list the query results
    cout << "NAME" << endl;
    
    for (result::const_iterator it = r.begin(); it != r.end(); ++it){
      cout << it[0].as<string>() << endl;
    }

  } catch(const pqxx_exception& pqe){
    cerr << pqe.base().what() << endl;
    cerr << "Performing rollback from query2()... " << endl;
    q2.abort();
  } catch (const std::exception &e){
    cerr << e.what() << endl;
    cerr << "Performing rollback from query2()... " << endl;
    q2.abort();
  }
}


void query3(connection *C, string team_name){

  work q3(*C);

  //Order from highest to lowest = descending 
  string sql = "SELECT PLAYER.FIRST_NAME, PLAYER.LAST_NAME FROM PLAYER, TEAM "
    "WHERE PLAYER.TEAM_ID = TEAM.TEAM_ID AND TEAM.NAME = " + q3.quote(team_name) +
    "ORDER BY PPG DESC;";
  
  try{
    result r(q3.exec(sql));

    cout << "FIRST_NAME LAST_NAME" << endl;

    for (result::const_iterator it = r.begin(); it != r.end(); ++it){
      cout << it[0].as<string>() << " " << it[1].as<string>() << endl;
    }
    
    //NOTE: Was doing it with the below method but realized I could use SQL with
    // and ORDER clause for much simpler implementation
    /*
    string sql = "SELECT PLAYER.FIRST_NAME, PLAYER.LAST_NAME, PLAYER.PPG FROM PLAYER, TEAM "
      "WHERE PLAYER.TEAM_ID = TEAM.TEAM_ID AND TEAM.NAME = " + q3.quote(team_name) + ";"; 

    std::vector< std::tuple<int, std::string, std::string> > results_to_sort;
    //int first because sort looks at first element of tuple
    
    for (result::const_iterator it = r.begin(); it != r.end(); ++it){
      results_to_sort.push_back(make_tuple(it[2].as<int>(), it[0].as<string>(), it[1].as<string>()));
      //cout << it[0].as<string>() << endl;
      //cout << it[1].as<string>() << endl;
      //cout << it[2].as<int>() << endl;
    }

    //using std::sort and reverse to insure order
    //std::sort(results_to_sort.begin(), results_to_sort.end());
    //std::reverse(results_to_sort.begin(), results_to_sort.end());
  
    for (int i = 0; i < results_to_sort.size() ; i++){
      cout << get<1>(results_to_sort[i]) << " " <<  get<2>(results_to_sort[i]) << endl;
    }
    */

  } catch(const pqxx_exception& pqe){
    cerr << pqe.base().what() << endl;
    cerr << "Performing rollback from query3()... " << endl;
    q3.abort();
  } catch (const std::exception &e){
    cerr << e.what() << endl;
    cerr << "Performing rollback from query3()... " << endl;
    q3.abort();
  }
}


void query4(connection *C, string team_state, string team_color){

  work q4(*C);

  string sql = "SELECT PLAYER.FIRST_NAME, PLAYER.LAST_NAME, PLAYER.UNIFORM_NUM FROM PLAYER, TEAM, "
    "STATE, COLOR WHERE PLAYER.TEAM_ID = TEAM.TEAM_ID AND TEAM.STATE_ID = STATE.STATE_ID AND "
    "TEAM.COLOR_ID = COLOR.COLOR_ID AND COLOR.NAME = " + q4.quote(team_color) + " AND "
    "STATE.NAME = " + q4.quote(team_state) + ";";

  try{

    result r(q4.exec(sql));

    cout << "FIRST_NAME LAST_NAME UNIFORM_NUM" << endl;
    
    for (result::const_iterator it = r.begin(); it != r.end(); ++it){
      cout << it[0].as<string>() << " " << it[1].as<string>() << " " << it[2].as<int>() << endl;
    }

  } catch(const pqxx_exception& pqe){
    cerr << pqe.base().what() << endl;
    cerr << "Performing rollback from query4()... " << endl;
    q4.abort();
  } catch (const std::exception &e){
    cerr << e.what() << endl;
    cerr << "Performing rollback from query4()... " << endl;
    q4.abort();
  }
}


void query5(connection *C, int num_wins){

  work q5(*C);

  string sql = "SELECT PLAYER.FIRST_NAME, PLAYER.LAST_NAME, TEAM.NAME, TEAM.WINS FROM PLAYER, TEAM "
    "WHERE PLAYER.TEAM_ID = TEAM.TEAM_ID AND TEAM.WINS > " + to_string(num_wins) + ";";         
  
  try{

    result r(q5.exec(sql));

    cout << "FIRST_NAME LAST_NAME NAME WINS" << endl;

    for (result::const_iterator it = r.begin(); it != r.end(); ++it){
      cout << it[0].as<string>() << " " << it[1].as<string>() << " " << it[2].as<string>()
	   << " " << it[3].as<int>() << endl;
    }

  } catch(const pqxx_exception& pqe){
    cerr << pqe.base().what() << endl;
    cerr << "Performing rollback from query5()... " << endl;
    q5.abort();
  } catch (const std::exception &e){
    cerr << e.what() << endl;
    cerr << "Performing rollback from query5()... " << endl;
    q5.abort();
  }
}
