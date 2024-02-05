pyinstaller -F main.pyw ^
    --add-data "About.txt;." ^
    --add-data "AllArtefactsNoAdventure.xml;." ^
    --add-data "AllSpellsNoAdventure.xml;." ^
    --add-data "RABMini*.xml;." ^
    --add-data "spells_*.xml;."