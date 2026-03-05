import { Routes, Route } from "react-router-dom";
import { Header } from "./components/Header.jsx";
import Drivers from "./pages/Drivers.jsx";
import Races from "./pages/Races.jsx";
import RaceResults from "./pages/RaceResults.jsx";
import Insights from "./pages/Insights.jsx";
import Notes from "./pages/Notes.jsx";
import Standings from "./pages/Standings.jsx";

export default function App(){
  return (
    <>
      <Header/>
      <div style={{position:"relative"}}>
        <div className="checker" />
        <div className="container">
          <Routes>
            <Route path="/" element={<Drivers/>}/>
            <Route path="/races" element={<Races/>}/>
            <Route path="/results" element={<RaceResults/>}/>
            <Route path="/standings" element={<Standings/>}/>
            <Route path="/insights" element={<Insights/>}/>
            <Route path="/notes" element={<Notes/>}/>
          </Routes>
        </div>
      </div>
    </>
  );
}