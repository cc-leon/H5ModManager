pyinstaller -F main.pyw ^
    --add-data "About.txt;." ^
    --add-data "AllArtefactsNoAdventure.xml;." ^
    --add-data "AllSpellsNoAdventure.xml;." ^
    --add-data "RAB*.xml;." ^
    --add-data "spells_*.xml;." ^
    --add-data "7za.exe;."