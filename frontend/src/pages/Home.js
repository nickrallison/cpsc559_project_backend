import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { FaEdit, FaTrash } from 'react-icons/fa';
import '../App.css';

const HTTPPORT = process.env.REACT_APP_HTTPPORT;

const Home = () => {
  const [newTask, setNewTask] = useState('');
  const [tasks, setTasks] = useState([]);
  const [statusMsg, setStatusMsg] = useState('');
  const [editingTask, setEditingTask] = useState(null);
  const [editedText, setEditedText] = useState('');
  const [deletingTask, setDeletingTask] = useState(null);

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
      .post(`http://${window.location.hostname}:${HTTPPORT}/objects`, [taskObject])
      .then(() => {
        setStatusMsg('Task added successfully!');
        setNewTask('');
        fetchTasks();
      })
      .catch((error) => {
        setStatusMsg('Error adding task');
        console.error('Error:', error);
      });
  };

  // Open the edit popup
  const openEditPopup = (task) => {
    setEditingTask(task);
    setEditedText(task.data);
  };

  // Close the edit popup
  const closeEditPopup = () => {
    setEditingTask(null);
    setEditedText('');
  };

  // Submit the edited task
  const submitEdit = () => {
    if (editedText.trim() === '') return;
    const updatedTask = { ...editingTask, data: editedText };
    axios
      .put(`http://${window.location.hostname}:${HTTPPORT}/objects`, updatedTask)
      .then(() => {
        setStatusMsg('Task updated successfully!');
        fetchTasks();
        closeEditPopup();
      })
      .catch((error) => {
        setStatusMsg('Error updating task');
        console.error('Update error:', error);
      });
  };

   // Open delete confirmation popup
  const openDeletePopup = (task) => {
    setDeletingTask(task);
  };

  // Close delete confirmation popup
  const closeDeletePopup = () => {
    setDeletingTask(null);
  };

  // Confirm delete action
  const confirmDelete = () => {
    axios
      .delete(`http://${window.location.hostname}:${HTTPPORT}/objects?userId=${deletingTask.user_id}&userMessageId=${deletingTask.user_message_id}`)
      .then(() => {
        setStatusMsg('Task deleted successfully!');
        fetchTasks();
        closeDeletePopup();
      })
      .catch((error) => {
        setStatusMsg('Error deleting task');
        console.error('Delete error:', error);
      });
  };

  // Polling to fetch tasks periodically (every 1 second)
  useEffect(() => {
    const pollingInterval = setInterval(() => {
      fetchTasks();
    }, 1000);
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
              <button onClick={() => openEditPopup(task)} className="icon-button edit-icon">
                <FaEdit />
              </button>
              <button onClick={() => openDeletePopup(task)} className="icon-button delete-icon">
                <FaTrash />
              </button>
            </div>
          </li>
        ))}
      </ul>

      {/* Modal Popup for Editing Task */}
      {editingTask && (
        <div className="modal-overlay">
          <div className="modal">
            <h3>Edit Task</h3>
            <input
              type="text"
              value={editedText}
              onChange={(e) => setEditedText(e.target.value)}
            />
            <div className="modal-actions">
              <button onClick={submitEdit}>Save</button>
              <button onClick={closeEditPopup}>Cancel</button>
            </div>
          </div>
        </div>
        
      )}
     {/* Modal Popup for Delete Confirmation */}
      {deletingTask && (
        <div className="modal-overlay">
          <div className="modal delete-modal">
            <h3>Confirm Delete</h3>
            <p>Are you sure you want to delete this task?</p>
            <div className="modal-actions">
              <button onClick={confirmDelete}>Delete</button>
              <button onClick={closeDeletePopup}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Home;
