/** esbuild bundles CSS imported for side effects; TypeScript needs to be told they exist. */
declare module "*.css" {
  const content: string
  export default content
}
