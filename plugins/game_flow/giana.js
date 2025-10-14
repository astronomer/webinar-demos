import GameComponent from "./game.js";

function PrinceComponent() {
    return GameComponent({
        id: "game-container",
        cssPath: "/gameflow/game.css",
        zipPath: "/gameflow/giana.zip",
        exePath: "./GIANAWEB.BAT"
    });
}

globalThis["Prince"] = PrinceComponent;
globalThis.AirflowPlugin = PrinceComponent;
