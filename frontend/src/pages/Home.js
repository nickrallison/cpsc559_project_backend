import React, { useState } from 'react';
import axios from 'axios';

const Home = () => {
  // State to hold form data
  const [userId, setUserId] = useState('');
  const [userMessageId, setUserMessageId] = useState('');
  const [data, setData] = useState('');
  const [responseMessage, setResponseMessage] = useState('');

  // Handle form submission
  const handleSubmit = (e) => {
    e.preventDefault();

    // Create the object to send
    const object = {
      user_id: parseInt(userId),
      user_message_id: parseInt(userMessageId),
      data: data,
    };

    // POST request to push data to the server
    axios
      .post('http://localhost:8080/objects', [object]) // Send data as an array
      .then((response) => {
        setResponseMessage('Data submitted successfully!');
        console.log(response.data);
      })
      .catch((error) => {
        setResponseMessage('Error submitting data');
        console.error('Error:', error);
      });
  };

  return (
    <div>
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
    </div>
  );
};

export default Home;
