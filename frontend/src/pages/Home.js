import React, {useState} from 'react';
import axios from 'axios';

const HTTPPORT = process.env.REACT_APP_HTTPPORT; 

const Home = () => {
  // State to hold form data
  const [userId, setUserId] = useState('');
  const [userMessageId, setUserMessageId] = useState('');
  const [data, setData] = useState('');
  const [responseMessage, setResponseMessage] = useState('');
  const [fetchedObjects, setFetchedObjects] = useState([]);

  // Handle form submission
  const handleSubmit = (e) => {
    e.preventDefault();

    // Create the object to send
    const object = {
      user_id: parseInt(userId),
      user_message_id: parseInt(userMessageId),
      data: data,
    };

    console.log(`http://localhost:${HTTPPORT}/objects`)
    // POST request to push data to the server
    axios
      .post(`http://localhost:${HTTPPORT}/objects`, [object]) // Send data as an array
      .then((response) => {
        setResponseMessage('Data submitted successfully!');
        console.log(response.data);
      })
      .catch((error) => {
        setResponseMessage('Error submitting data');
        console.error('Error:', error);
      });
  };

  // Handle fetching objects by User ID
  const handleGetObjects = () => {
    console.log(`http://localhost:${HTTPPORT}/objects?userId=1`)
    axios
      .get(`http://localhost:${HTTPPORT}/objects?userId=1`)
      .then((response) => {
        setFetchedObjects(response.data);
        setResponseMessage('');
      })
      .catch((error) => {
        setResponseMessage('Error fetching data');
        console.error('Error:', error);
      });
  };

  return (
    <div>
      <h1>Connected to Port {HTTPPORT}</h1>
      <h1>Submit Object</h1>
      <form onSubmit={handleSubmit}>
        <div>
          <label>User ID:</label>
          <input
            type="number"
            value={userId}
            onChange={(e) => setUserId(e.target.value)}
            required
          />
        </div>
        <div>
          <label>User Message ID:</label>
          <input
            type="number"
            value={userMessageId}
            onChange={(e) => setUserMessageId(e.target.value)}
            required
          />
        </div>
        <div>
          <label>Data:</label>
          <input
            type="text"
            value={data}
            onChange={(e) => setData(e.target.value)}
            required
          />
        </div>
        <button type="submit">Submit</button>
      </form>
      {responseMessage && <p>{responseMessage}</p>}
      <h1>Get Where User id is 1</h1>
      <button onClick={handleGetObjects} style={{ marginTop: "10px", backgroundColor: "#4C7355", color: "white", padding: "10px", border: "none", cursor: "pointer", borderRadius: "5px" }}>
        Get Items
      </button>
      <ul>
        {fetchedObjects.map((obj, index) => (
          <li key={index}>{JSON.stringify(obj)}</li>
        ))}
      </ul>
    </div>
  );
};

export default Home;
