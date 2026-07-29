import { render } from "solid-js/web"
import App from "./App"
import "./style.css"

const root = document.getElementById("root")
if (!root) throw new Error("Missing application root")

render(() => <App />, root)
