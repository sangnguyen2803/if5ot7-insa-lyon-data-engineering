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
INSERT INTO characters VALUES ('dragonborn', 'Tammy', $$['skill-survival', 'skill-perception']$$, 'draconic', $$[]$$, 'barbarian', '2', $$[7, 10, 18, 13, 18]$$);
INSERT INTO characters VALUES ('human', 'Brittany', $$['skill-intimidation', 'skill-insight']$$, 'primordial', $$['remove-curse', 'find-steed', 'create-food-and-water']$$, 'paladin', '3', $$[14, 12, 18, 18, 14]$$);
INSERT INTO characters VALUES ('elf', 'Deanna', $$['skill-religion', 'skill-persuasion']$$, 'abyssal', $$['contagion', 'bane', 'stone-shape']$$, 'cleric', '1', $$[17, 9, 14, 13, 19]$$);
INSERT INTO characters VALUES ('tiefling', 'Mitchell', $$['skill-religion', 'skill-stealth']$$, 'sylvan', $$[]$$, 'monk', '2', $$[16, 6, 13, 10, 19]$$);
INSERT INTO characters VALUES ('gnome', 'Brenda', $$['skill-deception', 'skill-religion']$$, 'undercommon', $$['hypnotic-pattern']$$, 'sorcerer', '2', $$[12, 19, 10, 12, 16]$$);
