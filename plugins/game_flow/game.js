const { useRef, useEffect } = React;
const e = React.createElement;

function GameComponent({ id, cssPath, zipPath, exePath }) {
    const rootRef = useRef(null);

    useEffect(() => {
        const styleLink = document.createElement("link");
        styleLink.rel = "stylesheet";
        styleLink.href = cssPath;
        document.head.appendChild(styleLink);

        const jqueryScript = document.createElement("script");
        jqueryScript.src = "https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js";
        jqueryScript.async = true;
        document.body.appendChild(jqueryScript);

        jqueryScript.onload = () => {
            const dosScript = document.createElement("script");
            dosScript.src = "https://js-dos.com/cdn/js-dos-api.js";
            dosScript.async = true;
            document.body.appendChild(dosScript);

            dosScript.onload = () => {
                if (typeof window.emulators === "undefined") {
                    window.emulators = {};
                }
                window.emulators.pathPrefix = cssPath.replace(/[^/]+$/, ""); // e.g. "/doom/"

                new window.Dosbox({
                    id,
                    onload: (dosbox) => dosbox.run(zipPath, exePath)
                });
            };
        };
    }, [id, cssPath, zipPath, exePath]);

    return e("div", { id, ref: rootRef });
}

export default GameComponent;
