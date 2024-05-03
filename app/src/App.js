import boogle_logo from './boogle_logo.jpg';
import './App.css';
import { useState, useEffect } from 'react';

let exampleQueryList = [{id: 1, url: 'https://www.brown.edu/'}, {id: 2, url: 'https://www.brown.edu/'}, {id: 2, url: 'https://www.brown.edu/'}];
let numResults = 10;
var nodeURL = 'http://localhost:8080/store/allquery'


function App() {
  const { search } = window.location;
  const query = new URLSearchParams(search).get('s');

  const [urlList, setURLList] = useState([]);
  const [queryDisplay, setQueryDisplay] = useState('');
  let results = false;

  useEffect(() => {
    const getResponse = async (query) => {
      if (!query) {
        return [];
      }
    
    const parseData = (data) => {
      let parsedArray = data["value"][1];
      if (!Array.isArray(parsedArray)) {
        results = false;
        return [];
      }
      results = true;
      parsedArray.pop();
      return parsedArray;
    }

      try {
        const options = {
          method: "PUT",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify([query, [], [], numResults])
        };

        const res = await fetch(nodeURL, options);
        const data = await res.json();
        console.log("DATA ", data);
        setURLList(parseData(data));
        if (results) {
          setQueryDisplay("Results for " + query);
        } else {
          setQueryDisplay("No results for " + query);
        }
        
      } catch (error) {
        console.error("Error fetching data:", error);
      }
    };

    getResponse(query);
  }, [query]);


  return (
    <div className="App">
      <header className="App-header">
        <img src={boogle_logo} className="App-logo" alt="logo" />
        <div>
        <p>
          Search for programming tutorials
        </p>
        <SearchBar />
        <p>{queryDisplay}</p>
        {urlList.map((url) => (                   
          <div className="box" key={url}>
            <a href={url}>{url}</a>
          </div>
        ))}
      </div>
      </header>
    </div>
  );
}

const SearchBar = () => (
  <form action="/" method="get">
    <input
        type="text"
        id="header-search"
        placeholder=""
        className="textbox"
        name="s" 
    />
      <button type="submit" className="submit-btn">Search</button>
  </form>
);

export default App;