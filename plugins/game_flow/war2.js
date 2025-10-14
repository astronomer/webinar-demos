import GameComponent from "./game.js";

function PrinceComponent() {
    return GameComponent({
        id: "game-container",
        cssPath: "/gameflow/game.css",
        zipPath: "/gameflow/war2.zip",
        exePath: "./WAR2WEB.BAT"
    });
}

globalThis["Warcraft"] = PrinceComponent;
globalThis.AirflowPlugin = PrinceComponent;
