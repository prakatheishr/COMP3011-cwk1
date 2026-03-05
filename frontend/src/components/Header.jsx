import { NavLink } from "react-router-dom";

export function Header(){
  return (
    <div className="header">
      <div className="header-inner">
        <div className="brand">
          <div className="brand-badge" />
          <div>F1 Insight Hub</div>
        </div>
        <div className="nav">
          <NavLink to="/" end className={({isActive}) => isActive ? "active" : ""}>Drivers</NavLink>
          <NavLink to="/races" className={({isActive}) => isActive ? "active" : ""}>Races</NavLink>
          <NavLink to="/results" className={({isActive}) => isActive ? "active" : ""}>Results</NavLink>
          <NavLink to="/standings" className={({isActive}) => isActive ? "active" : ""}>Standings</NavLink>
          <NavLink to="/insights" className={({isActive}) => isActive ? "active" : ""}>Insights</NavLink>
          <NavLink to="/notes" className={({isActive}) => isActive ? "active" : ""}>Notes</NavLink>
        </div>
      </div>
    </div>
  );
}