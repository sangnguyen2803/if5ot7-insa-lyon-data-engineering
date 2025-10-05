CREATE TABLE IF NOT EXISTS characters (
  race TEXT,
  name TEXT,
  proficiences TEXT,
  language TEXT,
  spells TEXT,
  class TEXT,
  levels TEXT,
  attributes TEXT
);
INSERT INTO characters VALUES ('halfling', 'Derek', $$['skill-insight', 'skill-intimidation']$$, 'dwarvish', $$['ice-storm', 'burning-hands', 'dancing-lights']$$, 'sorcerer', '3', $$[8, 13, 7, 16, 13]$$);
INSERT INTO characters VALUES ('tiefling', 'Sandra', $$['skill-performance', 'skill-deception', 'skill-insight', 'skill-perception']$$, 'draconic', $$[]$$, 'rogue', '3', $$[15, 15, 13, 7, 11]$$);
INSERT INTO characters VALUES ('dragonborn', 'Fred', $$['skill-acrobatics', 'skill-perception']$$, 'undercommon', $$[]$$, 'fighter', '3', $$[9, 7, 7, 11, 17]$$);
INSERT INTO characters VALUES ('gnome', 'Angela', $$['skill-perception', 'skill-survival', 'skill-insight']$$, 'halfling', $$['greater-invisibility', 'suggestion', 'light']$$, 'bard', '3', $$[11, 11, 16, 11, 9]$$);
INSERT INTO characters VALUES ('elf', 'Jenna', $$['skill-history', 'skill-arcana']$$, 'giant', $$['identify', 'banishment', 'telekinesis']$$, 'wizard', '2', $$[11, 9, 15, 15, 17]$$);
