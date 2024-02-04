pyinstaller -F main.pyw ^
    --add-data "About.txt;." ^
    --add-data "AllArtefactsNoAdventure.xml;." ^
    --add-data "AllSpellsNoAdventure.xml;." ^
    --add-data "RABMiniAcademy.xml;." ^
    --add-data "RABMiniFortress.xml;." ^
    --add-data "RABMiniHaven.xml;." ^
    --add-data "RABMiniPreserve.xml;."
