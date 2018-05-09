#include "exerciser.h"

//Code to test query functions 
void exercise(connection *C){

  //Add check
  for (int i = 1; i < 100; i++){
    add_player(C, i, i, "Player" + to_string(i), "last_name" + to_string(i),
	       10 + i, i, i, i, i, i);
    add_team(C, "Team" + to_string(i), i, i, i , 100 - i);
    add_state(C, "State" + to_string(i));
    add_color(C, "Color" + to_string(i));
  }


  //Incorrect add check
  add_player(C, 1, 0, "False'First ;", "FalseLast", 2, 3.9, 4, 5, 6, 7);
  add_player(C, 2, 0, "False First", "False'Last ;", 2.1, 3.9, 4.5, 5.7, 6.9, 7.7);
  add_team(C, "False'Team;", 2, 3.33, 4.6, 5.6); 
  add_state(C, "False: State; ' ;");
  add_color(C, "False; Color''");
  

  //Queries
  query1(C,
	 1, 24, 100,
	 0, 20, 100,
	 0, 0, 0,
	 0, 0, 0,
	 0, 0, 0,
	 1, 1, 20);
  
  query1(C,
	 0, 0, 100,
	 1, 19, 30,
	 0, 0, 0,
	 0, 0, 0,
	 0, 0, 0,
	 0, 1, 1);
  
  query1(C,
	 1, 0, 30,
	 1, 3, 10,
	 1, 4, 14,
	 1, 0, 10,
	 1, 2, 20,
	 1, 0, 10);

  query2(C, "Orange");
  query2(C, "LightBlue");
  query2(C, "Red");
  query2(C, "DarkBlue");
  query2(C, "MadeUpColor");

  query3(C, "Duke");
  query3(C, "Clemson");
  query3(C, "WakeForest");
  query3(C, "MadeUpTeam");

  query4(C, "NC", "DarkBlue");
  query4(C, "NC", "Gold");
  query4(C, "FL", "Green");
  query4(C, "FL", "Red");
  query4(C, "FL", "NotAColor");
  query4(C, "NotAState", "DarkBlue");
  
  query5(C, 10);
  query5(C, 8);
  query5(C, 13);
  
  
}
