function load_content(name) {
    fetch(name)
    .then((response) => response.text())
    .then((html) => {
        document.getElementById("content-" + name).innerHTML = html;
    })
    .catch((error) => {
        console.warn(error);
    });
}

function load_all() {
    load_content("filesystem");
    load_content("nodes");
    load_content("replication");
    load_content("users");
}

window.addEventListener('DOMContentLoaded', (event) => {
    load_all();
    window.setInterval(load_all, 5000);
});
