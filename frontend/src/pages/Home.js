
import React, { useState, useEffect } from 'react';
import { FaEdit, FaTrash } from 'react-icons/fa';
import axios from 'axios';
import '../App.css';

const HTTPPORT = process.env.REACT_APP_HTTPPORT;

const Home = () => {
  const [newTask, setNewTask] = useState('');
  const [tasks, setTasks] = useState([]);
  const [statusMsg, setStatusMsg] = useState('');

  // Fetch tasks for user 1
  const fetchTasks = () => {
    axios
      .get(`http://${window.location.hostname}:${HTTPPORT}/objects?userId=1`)
      .then((response) => {
        setTasks(response.data);
        setStatusMsg('');
      })
      .catch((error) => {
        setStatusMsg('Error fetching tasks');
        console.error('Fetch error:', error);
      });
  };

  // Add a new task
  const addTask = (e) => {
    e.preventDefault();
    if (!newTask.trim()) return;
    const taskObject = {
      user_id: 1,
      user_message_id: Date.now(), // using timestamp as a unique id
      data: newTask,
    };
    axios
      .post(`http://localhost:${HTTPPORT}/objects`, [taskObject])
      .then((response) => {
        setStatusMsg('Task added successfully!');
        setNewTask('');
        fetchTasks();
      })
      .catch((error) => {
        setStatusMsg('Error adding task');
        console.error('Error:', error);
      });
  };

  // Edit a task by sending a PUT request
  const editTask = (task) => {
    const updatedData = prompt('Edit task:', task.data);
    if (updatedData === null || updatedData.trim() === '') return;
    const updatedTask = { ...task, data: updatedData };
    axios
      .put(`http://localhost:${HTTPPORT}/objects`, updatedTask)
      .then((response) => {
        setStatusMsg('Task updated successfully!');
        fetchTasks();
      })
      .catch((error) => {
        setStatusMsg('Error updating task');
        console.error('Update error:', error);
      });
  };

  // Delete a task by sending a DELETE request
  const deleteTask = (task) => {
    axios
      .delete(`http://localhost:${HTTPPORT}/objects?userId=${task.user_id}&userMessageId=${task.user_message_id}`)
      .then(() => {
        setStatusMsg('Task deleted successfully!');
        fetchTasks();
      })
      .catch((error) => {
        setStatusMsg('Error deleting task');
        console.error('Delete error:', error);
      });
  };

  useEffect(() => {
    const pollingInterval = setInterval(() => {
      fetchTasks();
    }, 1000); // 1 second interval
    return () => clearInterval(pollingInterval);
  }, []);
  

  return (
    <div className="container">
      <h1>Shared To‑Do List</h1>
      <form className="task-form" onSubmit={addTask}>
        <input
          type="text"
          placeholder="Enter new task..."
          value={newTask}
          onChange={(e) => setNewTask(e.target.value)}
          required
        />
        <button type="submit">Add Task</button>
      </form>
      {statusMsg && <p className="status-msg">{statusMsg}</p>}
      <h2>Tasks</h2>
      <ul className="task-list">
        {tasks.map((task, index) => (
          <li key={index}>
            <span>{task.data}</span>
            <div className="actions">
            <button onClick={() => editTask(task)} className="icon-button edit-icon">
              <FaEdit />
            </button>
            <button onClick={() => deleteTask(task)} className="icon-button delete-icon">
              <FaTrash />
            </button>
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
};

export default Home;
