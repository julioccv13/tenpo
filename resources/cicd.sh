BUCKET="gs://prod-source-files"
gsutil -m cp -r 008a/* "$BUCKET/008a/"
gsutil -m cp -r 038/* "$BUCKET/038/"
gsutil -m cp -r 024/* "$BUCKET/024/"
gsutil -m cp -r 040/* "$BUCKET/040_Cliente_3M/"
gsutil -m cp -r 045/* "$BUCKET/045/"
gsutil -m cp -r 500/* "$BUCKET/500/"
gsutil -m cp -r 015/* "$BUCKET/015/"
gsutil -m cp -r 010/* "$BUCKET/010/"
gsutil -m cp -r 012/* "$BUCKET/012/"
gsutil -m cp -r 003/* "$BUCKET/003_Tenencia/"
gsutil -m cp -r 037/* "$BUCKET/037/"
gsutil -m cp -r 034/* "$BUCKET/034/"
gsutil -m cp -r 001/* "$BUCKET/001/"
gsutil -m cp -r 002/* "$BUCKET/002/"
gsutil -m cp -r 901/* "$BUCKET/901/"
gsutil -m cp -r 039/* "$BUCKET/039/"
gsutil -m cp -r 035/* "$BUCKET/035/"
gsutil -m cp -r 004/* "$BUCKET/004/"
gsutil -m cp -r 104/* "$BUCKET/104/"
gsutil -m cp -r 005/* "$BUCKET/005/"
gsutil -m cp -r 006/* "$BUCKET/006/"
gsutil -m cp -r 110/* "$BUCKET/110/"
gsutil -m cp -r 018/* "$BUCKET/018/"
gsutil -m cp -r 013/* "$BUCKET/013/"
gsutil -m cp -r 041/* "$BUCKET/041/"
gsutil -m cp -r 503/* "$BUCKET/503/"
gsutil -m cp -r 101/* "$BUCKET/101/"
gsutil -m cp -r 112/* "$BUCKET/112/queries/"
gsutil -m cp -r 114/* "$BUCKET/114/"
gsutil -m cp -r 115/* "$BUCKET/115/"
gsutil -m cp -r 116/* "$BUCKET/116/"
gsutil -m cp -r 117/* "$BUCKET/117/"
gsutil -m cp -r 118/* "$BUCKET/118/"
gsutil -m cp -r 119/* "$BUCKET/119/queries/"

FOLDER=$(pwd)
cd 200
zip -r 200.zip *
gsutil -m cp 200.zip "$BUCKET/200/"
rm 200.zip

cd $FOLDER