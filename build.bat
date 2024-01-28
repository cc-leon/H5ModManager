pyinstaller -F main.pyw ^
    --add-data "About.txt;." ^
    --add-data "AllArtefactsNoAdventure.xml;." ^
    --add-data "AllSpellsNoAdventure.xml;." ^
    --add-data "MiniAcademy.xml;." ^
    --add-data "MiniHaven.xml;." ^
    --add-data "MiniPreserve.xml;."
